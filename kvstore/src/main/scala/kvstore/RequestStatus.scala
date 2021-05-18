package kvstore

import akka.actor.{Actor, ActorRef}

import scala.collection.mutable

object RequestStatus {

  case class SecondaryConfirmed(actor:ActorRef)
  case class AllReplicated(id:Long)
}

class RequestStatus(id: Long, secondaries: Set[ActorRef]) extends Actor {

  import RequestStatus._

  val refs = scala.collection.mutable.Set[ActorRef]()
  secondaries.foreach(s => refs += s)

  def receive = {

    case SecondaryConfirmed(actor) =>
      println("Secondary confirmed")
      refs -= actor
      if (refs.isEmpty) sender ! AllReplicated(id)
  }
}
