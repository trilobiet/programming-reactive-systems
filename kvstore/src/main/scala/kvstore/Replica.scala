package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, Stash, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

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

  // acdhirr: send request to join
  override def preStart() = { arbiter !Join }

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

    case Insert(key, value, id) =>
      kv += (key->value)
      // TODO persist, then send ack when persistence succeeds
      sender() ! OperationAck(id)
    case Remove(key, id) =>
      kv -= key
      // TODO persist, then send ack when persistence succeeds
      sender() ! OperationAck(id)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id) // kv get returns Option

    case Persisted(key,id) => ???

    /* When a new replica joins the system, the primary receives a new Replicas message
    and must allocate a new actor of type Replicator for the new replica;
    when a replica leaves the system its corresponding Replicator must be terminated. */
    case Replicas(replicas) =>
      // TODO
      println("Replicas: " + replicas)
  }


  /* TODO Behavior for the replica (SECONDARY) role. */
  // - The secondary nodes must accept the lookup operation (Get) from clients following the Key-Value
  // protocol while respecting the guarantees described in “Guarantees for clients contacting the
  // secondary replica”.
  // - The replica nodes must accept replication events, updating their current state
  // (see “Replication Protocol”).
  val replica: Receive = {

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id) // kv get returns Option

    // messages coming from the Replicator:
    case Snapshot(key, _, seq) if seq < nextSeq =>
      // Do not process messages with a sequence number smaller
      // than the expected number, but do immediately acknowledge them
      sender() ! SnapshotAck(key, seq)

    case Snapshot(key, valueOption, seq) if seq == nextSeq =>

      valueOption match {
        case None => kv -= key
        case Some(value) => kv += (key->value)
      }

      val persistMsg: Persist = Persist(key,valueOption,seq)
      context.become( persisting(sender,persistMsg), discardOld = false ) // discard=false, so we can use 'unbecome'
      self ! persistMsg
  }

  def persisting(replicator: ActorRef, persistMsg: Persist): Receive = {

    // Secondary replica should already serve the received update while waiting for persistence!
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case persistMsg: Persist =>
      val persistence: ActorRef = context.actorOf(persistenceProps)
      context.watch(persistence)
      context.system.scheduler.scheduleWithFixedDelay(  // TODO stop schedule when Persisted arrives
        Duration.Zero,100.milliseconds,persistence,persistMsg
      )

    // Persistence succeeded
    case Persisted(key,id) =>
      replicator ! SnapshotAck(key, id)
      nextSeq += 1
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