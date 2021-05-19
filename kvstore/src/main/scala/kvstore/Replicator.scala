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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  context.watch(replica)

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var scheduledSnapshotMessages = Map.empty[String,Cancellable]
  // acdhirr: this is the primary (leader) Replica that must be informed
  // when replication is finished successfully.
  var primary:Option[ActorRef] = None

  
  /* TODO Behavior for the Replicator. */
  // The role of this Replicator actor is to accept update events, and propagate the changes
  // to its corresponding replica (i.e. there is exactly one Replicator per secondary replica).
  def receive: Receive = {

    // Replicate(key, valueOption, id) is sent to the Replicator to initiate the replication
    // of the given update to the key; in case of an Insert operation the valueOption will
    // be Some(value) while in case of a Remove operation it will be None. The sender of the
    // Replicate message shall be the Replica itself.
    case Replicate(key, valueOption, id) =>
      primary = Some(sender)
      // println(s"Replica in replicator ${self} is ${replica}")
      // Repeatedly send message to replica until canceled
      val cancellable = context.system.scheduler.scheduleWithFixedDelay(
          Duration.Zero, 100.milliseconds, replica, Snapshot(key, valueOption, id))
      scheduledSnapshotMessages += key->cancellable // add to map


    // SnapshotAck(key, seq) is the reply sent by the secondary replica to the Replicator as
    // soon as the update is persisted locally by the secondary replica. The replica might never
    // send this reply in case it is unable to persist the update. The sender of the SnapshotAck
    // message shall be the secondary Replica. The acknowledgement is sent immediately for requests
    // whose sequence number is less than the next expected number. The expected number is set to
    // the greater of the previously expected number and the sequence number just acknowledged,
    // incremented by one
    case SnapshotAck(key, seq) =>
      // println("SnapshotAck from " + sender)
      primary.map(_ ! Replicated(key, seq))  // primary is Option
      scheduledSnapshotMessages(key).cancel() // cancel this scheduled message
      scheduledSnapshotMessages -= key

  }

  override def postStop(): Unit = {
    println("REPLICATOR STOPPING " + self)
    context.stop(replica)
  }

}


