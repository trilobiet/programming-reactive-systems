package kvstore

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import kvstore.Replicator.{Replicate, Replicated}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object Disseminator {

  case class AllReplicated(id:Long)
  case class DisseminationTimeout(id:Long)
}

class Disseminator( replicateMsg:Replicate, secondaries: Set[ActorRef]) extends Actor {

  import Disseminator._

  context.setReceiveTimeout(1000.milliseconds)

  if (secondaries.isEmpty) context.parent ! AllReplicated(replicateMsg.id)
  else secondaries.foreach( s => {
    println(s"replicating to ${s}")
    s ! replicateMsg
  })

  var refs = scala.collection.mutable.Set[ActorRef]()
  secondaries.foreach(s => refs += s)

  def receive = {

    case Replicated(key, id) =>
      println(s"Secondary ${id} confirmed")
      refs -= sender
      if (refs.isEmpty) context.parent ! AllReplicated(id)

    case ReceiveTimeout =>
      context.parent ! DisseminationTimeout(replicateMsg.id)
  }
}