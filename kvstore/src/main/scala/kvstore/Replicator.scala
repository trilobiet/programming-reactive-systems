package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, Terminated}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher

  var scheduledSnapshotMessages = Map.empty[String,Cancellable]

  // This is the primary (leader) Replica that must be informed
  // when replication is finished successfully.
  var primary:Option[ActorRef] = None

  // The Snapshot message provides a sequence number (seq) to enforce ordering between the updates.
  // Updates for a given secondary replica must be processed in contiguous ascending sequence
  // number order; this ensures that updates for every single key are applied in the correct order.
  // Each Replicator uses its own number sequence starting at zero.
  private var seqCounter = -1L
  def nextSeq() = {
    seqCounter += 1
    seqCounter
  }

  /* Behavior for the Replicator. */
  // The role of this Replicator actor is to accept update events, and propagate the changes
  // to its corresponding replica (i.e. there is exactly one Replicator per secondary replica).
  def receive: Receive = {

    // Replicate(key, valueOption, id) is sent to the Replicator to initiate the replication
    // of the given update to the key; in case of an Insert operation the valueOption will
    // be Some(value) while in case of a Remove operation it will be None. The sender of the
    // Replicate message shall be the Replica itself.
    case Replicate(key, valueOption, id) =>
      primary = Some(sender())
      // Repeatedly send message to replica until canceled
      val cancellable = context.system.scheduler.scheduleWithFixedDelay(
          Duration.Zero, 100.milliseconds, replica, Snapshot(key, valueOption, nextSeq()))
      scheduledSnapshotMessages += key->cancellable // add to map

    // SnapshotAck(key, seq) is the reply sent by the secondary replica to the Replicator as
    // soon as the update is persisted locally by the secondary replica. The replica might never
    // send this reply in case it is unable to persist the update. The sender of the SnapshotAck
    // message shall be the secondary Replica. The acknowledgement is sent immediately for requests
    // whose sequence number is less than the next expected number. The expected number is set to
    // the greater of the previously expected number and the sequence number just acknowledged,
    // incremented by one.
    case SnapshotAck(key, seq) =>
      primary.map(_ ! Replicated(key, seq))  // primary is Option
      cancelSnapshotMessage(key)

    case Terminated(_) =>
      context.stop(replica)
  }

  // Cancel the scheduled sending of Snapshot messages once the acknowledgement is received
  def cancelSnapshotMessage(key:String): Unit =
    if( scheduledSnapshotMessages.keySet.contains(key) ) {
      scheduledSnapshotMessages(key).cancel() // cancel this scheduled message
      scheduledSnapshotMessages -= key
    }

  override def postStop(): Unit = {
    // println("REPLICATOR STOPPING " + self)
    context.stop(replica)
  }

}


