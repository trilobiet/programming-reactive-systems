package kvstore

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, ReceiveTimeout, Stash, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout
import kvstore.Disseminator.{AllReplicated, DisseminationTimeout}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case class IsPersisted(key: String, id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // the data: key-value pairs
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // secondary nodes expect this seq number for updates
  var nextSeq = 0L
  var persistTask: Option[Cancellable] = None

  // who made which request
  var clients = Map.empty[Long,ActorRef]
  var complete = scala.collection.mutable.Map.empty[Long,Int]

  // acdhirr: after creation send request to join
  override def preStart() = { arbiter !Join }

  context.setReceiveTimeout(1000.milliseconds)

  // Stop the persistence actor on error
  override val supervisorStrategy = OneForOneStrategy() {
    case e: PersistenceException =>
      println("Hell, no: " + e.getMessage)
      Stop
  }

  // When your actor starts, it must send a Join message to the Arbiter and then choose
  // between primary or secondary behavior according to the reply of the Arbiter to the
  // Join message (a JoinedPrimary or JoinedSecondary message).
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for the leader (PRIMARY) role. */
  // - The primary must accept update and lookup operations from clients following the Key-Value
  // protocol like Insert, Remove or Get as it is described in the “Clients and The KV Protocol”
  // section, respecting the consistency guarantees described in “Guarantees for clients
  // contacting the primary replica”.
  // - The primary must replicate changes to the secondary replicas of the system. It must also
  // react to changes in membership (whenever it gets a Replicas message from the Arbiter) and start
  // replicating to newly joined nodes, and stop replication to nodes that have left; the latter
  // implies terminating the corresponding Replicator actor.
  // More details can be found in the section “Replication Protocol”.
  val leader: Receive = {

    case DisseminationTimeout(id) =>
      println(s"waaaaaaaaaaaaaaaaaay too long ${id}")
      clients(id) ! OperationFailed(id)

    case IsPersisted(key,id) =>
      println("primary is persisted")
      complete(id) += 1
      if (complete(id) == 2) clients(id) ! OperationAck(id)

    case Insert(key, value, id) =>
      kv += (key->value)
      clients += id->sender()
      complete += id->0

      // Persist and replicate
      val persistMsg: Persist = Persist(key,Some(value),id)
      context.become( persisting(persistMsg), discardOld = false ) // discard=false, so we can use 'unbecome'
      self ! persistMsg
      // sender() ! OperationAck(id)

      println("bbbb")
      val disseminator: ActorRef = context.actorOf(Props(new Disseminator(Replicate(key,Some(value),id),secondaries.values.toSet)))

    case Remove(key, id) =>
      kv -= key
      clients += id->sender()
      complete += id->0

      // Persist and replicate, then send ack when these succeed
      val persistMsg: Persist = Persist(key,None,id)
      context.become( persisting(persistMsg), discardOld = false ) // discard=false, so we can use 'unbecome'
      self ! persistMsg
      // sender() ! OperationAck(id)

      val disseminator: ActorRef = context.actorOf(Props(new Disseminator(Replicate(key,None,id),secondaries.values.toSet)))

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id) // kv get returns Option

    case AllReplicated(id) =>
      println(id + " replicated")
      complete(id) += 1
      if (complete(id) == 2) clients(id) ! OperationAck(id)


    /* When a new replica joins the system, the primary receives a new Replicas message
    and must allocate a new actor of type Replicator for the new replica; when a replica
    leaves the system its corresponding Replicator must be terminated.
    The role of this Replicator actor is to accept update events, and propagate the changes
    to its corresponding replica (i.e. there is exactly one Replicator per secondary replica).
    Also, notice that at creation time of the Replicator, the primary must forward update
    events for every key-value pair it currently holds to this Replicator.
    */
    case Replicas(replicas) =>

      // remove removed replicas from secondaries
      secondaries.foreach(sec => {
        val secondary = sec._1
        if (!replicas.contains(secondary)) {
          val replicator = secondaries(secondary)
          //replicator ! Stop
          secondaries -= secondary
          //secondary ! Stop
        }
      })

      // Add new secondaries
      replicas.filter(r => r.actorRef != self.actorRef).foreach( replica =>
        if (!secondaries.contains(replica)) {
          // allocate a new actor of type Replicator for the new replica
          val replicator = context.actorOf(Props(new Replicator(replica)))
          secondaries += replica->replicator
          // send all historic messages to new replica
          kv.foreach(kv => {

            // println("replicator ! " + Replicate(kv._1, Some(kv._2), nextSeq))
            replicator ! Replicate(kv._1, Some(kv._2), nextSeq)
            nextSeq += 1
          })
        }
      )
  }



  /* TODO Behavior for the replica (SECONDARY) role. */
  // - The secondary nodes must accept the lookup operation (Get) from clients following the Key-Value
  // protocol while respecting the guarantees described in “Guarantees for clients contacting the
  // secondary replica”.
  // - The replica nodes must accept replication events, updating their current state
  // (see “Replication Protocol”).
  val replica: Receive = {

    case Get(key, id) =>
      println(s"GET ($key,$id)=${kv.get(key)}")
      sender() ! GetResult(key, kv.get(key), id) // kv get returns Option

    // Do not process messages with a sequence number smaller
    // than the expected number, but do immediately acknowledge them
    case Snapshot(key, _, seq) if seq < nextSeq =>
      sender() ! SnapshotAck(key, seq)

    // Update and persist when expected seq number arrives
    case Snapshot(key, valueOption, seq) if seq == nextSeq =>

      secondaries += self->sender // map replicator to this replica

      valueOption match {
        case None => kv -= key
        case Some(value) => kv += (key->value)
      }

      val persistMsg: Persist = Persist(key,valueOption,seq)
      context.become( persisting(persistMsg), discardOld = false ) // discard=false, so we can use 'unbecome'
      self ! persistMsg

    case IsPersisted(key,id) =>
      secondaries(self) ! SnapshotAck(key, id) // forward to replicator
  }

  def persisting(persistMsg: Persist): Receive = {

    // Secondary replica should already serve the received update while waiting for persistence!
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case persistMsg: Persist =>
      val persistence: ActorRef = context.actorOf(persistenceProps)
      // context.watch(persistence)
      persistTask = Some(context.system.scheduler.scheduleWithFixedDelay(  // to satisfy 2nd test of Step4, switch off when persisted arrives
        Duration.Zero,100.milliseconds,persistence,persistMsg
      ))

    // Persistence time out
    case ReceiveTimeout =>
      println("timeout!!!!!!")
      clients(persistMsg.id) ! OperationFailed(persistMsg.id)

    // Persistence succeeded
    case Persisted(key,id) =>
      self ! IsPersisted(key, id) // forward to replicator
      nextSeq += 1 // increase counter
      sender ! Stop
      persistTask.map(_.cancel()) // stop repeating messages
      context.unbecome()  // revert to previous behaviour
      unstashAll()

    case _ => stash()

  }

}

/*
Snapshot(key, valueOption, seq) is sent by the Replicator to the appropriate secondary
replica to indicate a new state of the given key. valueOption has the same meaning as for
Replicate messages. The sender of the Snapshot message shall be the Replicator.
The Snapshot message provides a sequence number (seq) to enforce ordering between the updates.
Updates for a given secondary replica must be processed in contiguous ascending sequence number order;
this ensures that updates for every single key are applied in the correct order.
Each Replicator uses its own number sequence starting at zero. When a snapshot arrives at a Replica with
a sequence number which is greater than the currently expected number, then that snapshot must be ignored
(meaning no state change and no reaction). When a snapshot arrives at a Replica with a sequence number
which is smaller than the currently expected number, then that snapshot must be ignored and immediately
acknowledged as described below. The sender reference when sending the Snapshot message must be the Replicator
actor (not the primary replica actor or any other).
*/