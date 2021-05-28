package kvstore

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import kvstore.Replicator.{Replicate, Replicated}
import scala.concurrent.duration.DurationInt

object Disseminator {

  case class Disseminate()
  case class IsDisseminated(id:Long)
  case class DisseminationTimeout(id:Long)
  case class ReplicaRemoved(replicator:ActorRef)
}

/**
  * Disseminator takes care of sending out the replicate message to all
  * given replicators and reports back to parent (leader Replica) when done.
  *
  * @param replicateMsg
  * @param replicators
  */
class Disseminator(replicateMsg:Replicate, replicators: Set[ActorRef]) extends Actor {

  import Disseminator._

  context.setReceiveTimeout(1000.milliseconds)

  /* Mutable set containing replicators that await replication.
  Once this set is empty, we're done and can reply to the disseminators parent. */
  var reps = scala.collection.mutable.Set[ActorRef]() ++= replicators

  private def replyReady() = {
    context.parent ! IsDisseminated(replicateMsg.id)
    context.stop(self)
  }

  def receive = {

    case Disseminate =>
      if (reps.isEmpty) replyReady() // No replicators to replicate to
      else reps.foreach(_ ! replicateMsg) // Send replicate message to all replicators

    case Replicated(key, id) =>
      reps -= sender() // replicator done
      if (reps.isEmpty) replyReady()

    /* Tells the disseminator that a Replicator is no longer taking part
       so it can be removed from the replicators set.
    */
    case ReplicaRemoved(replicator) =>
      reps -= replicator
      if (reps.isEmpty) replyReady()

    case ReceiveTimeout =>
      context.parent ! DisseminationTimeout(replicateMsg.id)
      context.stop(self)
  }

}