package kvstore

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, ReceiveTimeout, Stash, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import kvstore.Disseminator.{IsDisseminated, Disseminate, DisseminationTimeout, ReplicaRemoved}
import scala.util.Random

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

  def rnd: Integer = Random.between(100, 999)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  case class UpdateStatus(sender: ActorRef, var isPersisted: Boolean, var isDisseminated: Boolean) {
    def isComplete = isPersisted && isDisseminated
  }
  // Maps id -> updateStatus
  var updates = Map.empty[Long,UpdateStatus]

  // the data: key-value pairs
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  // var replicators = Set.empty[ActorRef]
  // secondary nodes expect this seq number for updates
  var nextSeq = 0L
  var persistTask: Option[Cancellable] = None

  // COMMON METHODS ----------------------------------------------
  private def disseminate(key: String, valueOption: Option[String], id: Long) = {
    val disseminator: ActorRef = context.actorOf(Props(new Disseminator(Replicate(key,valueOption,id),secondaries.values.toSet)),s"disseminator-${rnd}")
    disseminator ! Disseminate
  }

  private def persist(key: String, valueOption: Option[String], id: Long) = {
    context.become( persisting(id), discardOld = false ) // discard=false, so we can use 'unbecome'
    self ! Persist(key,valueOption,id)
  }

  private def handleComplete(id: Long) = {
    updates(id).sender ! OperationAck(id)
    nextSeq += 1 // increase counter
  }
  // -------------------------------------------------------------

  // acdhirr: after creation send request to join
  override def preStart() = { arbiter !Join }

  // Persistence actor
  val persistence: ActorRef = context.actorOf(persistenceProps,s"persistence-${rnd}")
  context.watch(persistence)
  override val supervisorStrategy = OneForOneStrategy() {
    case e: PersistenceException => Restart
  }

  context.setReceiveTimeout(1000.milliseconds)

  // When your actor starts, it must send a Join message to the Arbiter and then choose
  // between primary or secondary behavior according to the reply of the Arbiter to the
  // Join message (a JoinedPrimary or JoinedSecondary message).
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* Behavior for the leader (PRIMARY) role. */
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
      updates(id).sender ! OperationFailed(id)

    case IsPersisted(key,id) =>
      updates(id).isPersisted = true
      if (updates(id).isComplete) handleComplete(id)

    case IsDisseminated(id) =>
      updates(id).isDisseminated = true
      if (updates(id).isComplete) handleComplete(id)

    case Insert(key, value, id) =>
      kv += (key->value)
      updates += id->UpdateStatus(sender,false,false)
      disseminate(key,Some(value),id)
      persist(key,Some(value),id)

    case Remove(key, id) =>
      kv -= key
      updates += id->UpdateStatus(sender,false,false)
      disseminate(key,None,id)
      persist(key,None,id)

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id) // kv get returns Option


    /* When a new replica joins the system, the primary receives a new Replicas message
    and must allocate a new actor of type Replicator for the new replica; when a replica
    leaves the system its corresponding Replicator must be terminated.
    The role of this Replicator actor is to accept update events, and propagate the changes
    to its corresponding replica (i.e. there is exactly one Replicator per secondary replica).
    Also, notice that at creation time of the Replicator, the primary must forward update
    events for every key-value pair it currently holds to this Replicator.
    */
    case Replicas(replicas) =>

      val secReplicas = replicas.filterNot(_.actorRef==self)
      val newReplicas: Set[ActorRef] = secReplicas.diff(secondaries.keys.toSet)
      val removedReplicas: Set[ActorRef] = secondaries.keys.toSet.diff(secReplicas)

      // remove and stop removed replicas
      removedReplicas.foreach(r => {
          context.stop(secondaries(r)) // associated Replicator
          context.stop(r) // Todo make dependent (watch?)
          context.children.foreach(_ ! ReplicaRemoved(secondaries(r)))
          secondaries -= r
        })

      // Add new replicas
      newReplicas.foreach( r => secondaries += r -> context.actorOf(Props(new Replicator(r)),s"replicator-${rnd}"))
      val newSecondaries: Map[ActorRef, ActorRef] = secondaries.view.filterKeys(newReplicas).toMap

      kv.zipWithIndex.foreach { case((k,v),i) => {
        updates += i.toLong->UpdateStatus(sender,true,false) // already persisted!
        val disseminator: ActorRef = context.actorOf(Props(new Disseminator(Replicate(k, Some(v), i), newSecondaries.values.toSet)),s"disseminator-${rnd}")
        disseminator ! Disseminate
      }}
  }

  /* Behavior for the replica (SECONDARY) role. */
  // - The secondary nodes must accept the lookup operation (Get) from clients following the Key-Value
  // protocol while respecting the guarantees described in “Guarantees for clients contacting the
  // secondary replica”.
  // - The replica nodes must accept replication events, updating their current state
  // (see “Replication Protocol”).
  val replica: Receive = {

    case Get(key, id) =>
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

      persist(key,valueOption,seq)

    case IsPersisted(key,id) =>
      secondaries(self) ! SnapshotAck(key, id) // forward to replicator
      nextSeq += 1 // increase counter

  }

  /* Behavior when persisting. */
  def persisting(id:Long): Receive = leader orElse {

    // Replica should already serve the received update while waiting for persistence!
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case message: Persist =>
      persistTask = Some(context.system.scheduler.scheduleWithFixedDelay(  // to satisfy 2nd test of Step4, switch off when persisted arrives
        Duration.Zero,100.milliseconds,persistence,message
      ))

    // Persistence time out
    case ReceiveTimeout =>
      println("Persistence timeout!!!!!!")
      updates(id).sender ! OperationFailed(id)

    // Persistence succeeded
    case Persisted(key,id) =>
      self ! IsPersisted(key, id) // forward to replicator
      persistTask.map(_.cancel()) // stop repeating messages
      context.unbecome()  // revert to previous behaviour
      unstashAll()

    case _ => stash()

  }

  override def postStop(): Unit = {
    println("REPLICA STOPPING " + self)
  }

}

