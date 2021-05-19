package kvstore

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import kvstore.Replicator.{Replicate, Replicated}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object Disseminator {

  case class Disseminate()
  case class AllReplicated(id:Long)
  case class DisseminationTimeout(id:Long)
}

class Disseminator(replicateMsg:Replicate, replicators: Set[ActorRef]) extends Actor {

  import Disseminator._

  context.setReceiveTimeout(1000.milliseconds)

  var reps = scala.collection.mutable.Set[ActorRef]()
  replicators.foreach(r => reps += r)

  def receive = {

    case Disseminate =>
      if (replicators.isEmpty) {
        //println(s"Nothing to disseminate ${replicateMsg.id}")
        context.parent ! AllReplicated(replicateMsg.id)
      }
      else replicators.foreach(s => {
        //println(s"replicating ${replicateMsg} to Replicator ${s} ")
        s ! replicateMsg
      })

    case Replicated(key, id) =>
      //println(s"Secondary ${id} confirmed")
      reps -= sender
      if (reps.isEmpty) {
        context.parent ! AllReplicated(id)
        //println("ALL DONE FOR THIS DISSEMINATOR")
        context.stop(self)
      }

    case ReceiveTimeout =>
      //println(self + ": we have a situation here " + context.parent)
      context.parent ! DisseminationTimeout(replicateMsg.id)
      context.stop(self)
  }
}