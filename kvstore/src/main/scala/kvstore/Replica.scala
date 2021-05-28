package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, Props, ReceiveTimeout, Stash}
import kvstore.Arbiter._
import kvstore.Disseminator.{Disseminate, DisseminationTimeout, IsDisseminated, ReplicaRemoved}

import scala.concurrent.duration._
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
  case class IsNotPersisted(id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  def rnd: Integer = Random.between(100, 999)
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  // Maps id -> updateStatus
  var updates = Map.empty[Long,UpdateStatus]

  // The data: key-value pairs
  var kv = Map.empty[String, String]

  // A map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // Secondary nodes expect this seq number for updates
  var nextSeq = 0L
  var persistTask: Option[Cancellable] = None

  // Status objects containing progress status of a request to the primary (leader) replica
  case class UpdateStatus(sender: ActorRef, var isPersisted: Boolean, var isDisseminated: Boolean) {
    def isComplete = isPersisted && isDisseminated
  }

  // COMMON METHODS ----------------------------------------------

  // Disseminate an update to a set of replicators
  private def disseminate(key: String, valueOption: Option[String], id: Long, replicators:Set[ActorRef]) = {
    val disseminator: ActorRef = context.actorOf(
      Props(new Disseminator(Replicate(key,valueOption,id),secondaries.values.toSet)),s"disseminator-${rnd}")
    disseminator ! Disseminate
  }

  // Persist an update
  private def persist(key: String, valueOption: Option[String], id: Long) = {
    context.become( persisting(id), discardOld = false ) // discard=false, so we can use 'unbecome'
    self ! Persist(key,valueOption,id)
  }
  // -------------------------------------------------------------

  context.setReceiveTimeout(1000.milliseconds)

  // When your actor starts, it must send a Join message to the Arbiter and then choose
  // between primary or secondary behavior according to the reply of the Arbiter to the
  // Join message (a JoinedPrimary or JoinedSecondary message).
  override def preStart() = { arbiter ! Join }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // Persistence actor for this replica
  val persistence: ActorRef = context.actorOf(persistenceProps,s"persistence-${rnd}")
  context.watch(persistence)
  override val supervisorStrategy = OneForOneStrategy() {
    case e: PersistenceException =>  Restart
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
      if (updates(id).isComplete) updates(id).sender ! OperationAck(id)

    case IsNotPersisted(id) =>
      updates(id).sender ! OperationFailed(id)

    case IsDisseminated(id) =>
      updates(id).isDisseminated = true
      if (updates(id).isComplete) updates(id).sender ! OperationAck(id)

    case Insert(key, value, id) =>
      kv += (key->value)
      updates += id->UpdateStatus(sender(),false,false)
      disseminate( key, Some(value), id, replicators=secondaries.values.toSet )
      persist( key, Some(value), id )

    case Remove(key, id) =>
      kv -= key
      updates += id->UpdateStatus(sender(),false,false)
      disseminate( key, None, id, replicators=secondaries.values.toSet )
      persist( key, None, id )

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

      val secReplicas = replicas.filterNot(_ == self) // leave out the primary
      val newReplicas: Set[ActorRef] = secReplicas.diff(secondaries.keys.toSet)
      val removedReplicas: Set[ActorRef] = secondaries.keys.toSet.diff(secReplicas)

      // remove and stop removed replicas
      removedReplicas.foreach(r => {
          context.stop(secondaries(r)) // associated Replicator
          context.stop(r)
          // tell disseminators that Replicator is gone
          context.children.foreach(_ ! ReplicaRemoved(secondaries(r)))
          secondaries -= r
        })

      // Add new replicas
      newReplicas.foreach( r => secondaries += r -> context.actorOf(Props(new Replicator(r)),s"replicator-${rnd}"))
      val newSecondaries: Map[ActorRef, ActorRef] = secondaries.view.filterKeys(newReplicas).toMap

      kv.zipWithIndex.foreach { case((k,v),i) => {
        updates += i.toLong->UpdateStatus(sender(),true,false) // already persisted!
        disseminate( k, Some(v), i, replicators=newSecondaries.values.toSet )
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
    // than the expected number, but do immediately acknowledge them.
    case Snapshot(key, _, seq) if seq < nextSeq =>
      sender() ! SnapshotAck(key, seq)

    // Update and persist when expected seq number arrives.
    case Snapshot(key, valueOption, seq) if seq == nextSeq =>

      secondaries += self->sender() // map replicator to this replica

      valueOption match {
        case None => kv -= key
        case Some(value) => kv += (key->value)
      }

      nextSeq += 1 // increase sequence counter
      persist(key,valueOption,seq)

    case IsPersisted(key,id) =>
      secondaries(self) ! SnapshotAck(key, id) // forward to replicator

    case IsNotPersisted(id) =>
      secondaries(self) ! OperationFailed(id)

  }

  /* Behavior when persisting. */
  def persisting(id:Long): Receive = {

    // Replica should already serve the received update while waiting for persistence!
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case message: Persist =>
      persistTask = Some(context.system.scheduler.scheduleWithFixedDelay(  // to satisfy 2nd test of Step4, switch off when persisted arrives
        Duration.Zero,100.milliseconds,persistence,message
      ))

    // Persistence time out (does this ever happen?)
    case ReceiveTimeout =>
      persistTask.map(_.cancel()) // stop repeating messages
      context.unbecome()  // revert to previous behaviour
      self ! IsNotPersisted(id)

    // Persistence succeeded
    case Persisted(key,id) =>
      persistTask.map(_.cancel()) // stop repeating messages
      context.unbecome()  // revert to previous behaviour
      self ! IsPersisted(key, id) // forward to replicator
      unstashAll()

    case _ => stash()

  }

}

