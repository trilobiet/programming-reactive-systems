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

class Disseminator(replicateMsg:Replicate, replicators: Set[ActorRef]) extends Actor {

  import Disseminator._

  context.setReceiveTimeout(1000.milliseconds)

  var reps = scala.collection.mutable.Set[ActorRef]()
  replicators.foreach(r => reps += r)


  def receive = {

    case Disseminate =>
      if (reps.isEmpty) reply(replicateMsg.id)
      else replicate

    case Replicated(key, id) =>
      reps -= sender
      if (reps.isEmpty) reply(id)

    /* Tells the disseminator that a Replicator is no longer taking part */
    case ReplicaRemoved(replicator) =>
      reps -= replicator
      if (reps.isEmpty) reply(replicateMsg.id)

    case ReceiveTimeout =>
      context.parent ! DisseminationTimeout(replicateMsg.id)
      context.stop(self)
  }

  def reply(id: Long) = {
    context.parent ! IsDisseminated(replicateMsg.id)
    context.stop(self)
  }

  // Send Replicate message to all known Replicators
  def replicate() =
    reps.foreach(s => {
      //  println(s"replicating ${replicateMsg} to Replicator ${s} ")
      s ! replicateMsg
    })


}