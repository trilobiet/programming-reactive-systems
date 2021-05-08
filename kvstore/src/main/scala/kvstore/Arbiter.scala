package kvstore

import akka.actor.{ActorRef, Actor}

object Arbiter {

  // New replicas must first send a Join message to the Arbiter
  // signaling that they are ready to be used.
  case object Join
  // The Join message will be answered by either a JoinedPrimary or JoinedSecondary
  // message indicating the role of the new node; the answer will be sent to
  // the sender of the Join message. The first node to join will get the primary
  // role, other subsequent nodes are assigned the secondary role.
  case object JoinedPrimary
  case object JoinedSecondary

  /**
   * This message contains all replicas currently known to the arbiter, including the primary.
   */
  case class Replicas(replicas: Set[ActorRef])
}

class Arbiter extends Actor {
  import Arbiter._
  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  def receive = {
    case Join =>
      if (leader.isEmpty) {
        leader = Some(sender)
        replicas += sender
        sender ! JoinedPrimary
      } else {
        replicas += sender
        sender ! JoinedSecondary
      }
      /* The arbiter will send a Replicas message to the primary replica whenever it
      receives the Join message; for this reason the sender of the Join message
      must be the replica Actor itself. This message contains the set of available
      replica nodes including the primary and all the secondaries. */
      leader foreach (_ ! Replicas(replicas))
  }

}
