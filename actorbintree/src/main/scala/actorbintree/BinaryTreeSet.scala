/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

/**
  * The BinaryTreeSet represents the whole binary tree.
  * This is also the only actor that is explicitly created by the user
  * and the only actor the user sends messages to.
  */
class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root: ActorRef = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation:Operation => root ! operation
    // start gc and pass in a new root to contain the cleaned up tree
    case GC =>
      val newRoot = createRoot
      root ! CopyTo( newRoot )
      context.become( garbageCollecting(newRoot) ) // delegate incoming messages to queue
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation:Operation => pendingQueue = pendingQueue.enqueue(operation)
    case CopyFinished =>
      root ! PoisonPill  // Stop the old root
      root = newRoot
      // return to normal and send out pending messages
      context.become(normal)
      pendingQueue.foreach(op => root ! op)
      pendingQueue = Queue.empty[Operation] // flush queue
  }
}

/**
  * Represents a node on the binary tree
  */
object BinaryTreeNode {

  trait Position
  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Contains(requester: ActorRef, id: Int, elemSearch: Int) =>

      if (elemSearch == elem ) {
        println(s"ContainsResult(${id}, ${true && !removed})")
        requester ! ContainsResult(id, true && !removed)
      }
      else {
        val pos: Position = if (elemSearch > elem) Right else Left
        subtrees.get(pos) match {
          case None => {
            println(s"ContainsResult(${id}, false)")
            requester ! ContainsResult(id, false)
          } // notify requester that elemSearch is not present
          case Some(node) => node ! Contains(requester,id,elemSearch) // delegate to appropriate child node
        }
      }


    case Insert(requester: ActorRef, id: Int, elemNew: Int) =>

      if (elemNew == elem) {
        removed = false // Undo potential removal flag, we insert this element anew!
        requester ! OperationFinished(id) // already present, notify requester
      }
      else {
        // Where would that element be inserted? (Left or Right)
        val pos: Position = if (elemNew > elem) Right else Left
        subtrees.get(pos) match {
          case None => { // there is still room available
            val node = context.actorOf( props(elemNew,false) )
            subtrees += (pos -> node) // add node at pos
            println(s"OperationFinished(${id})")
            requester ! OperationFinished(id) // notify requester
          }
          case Some(node) => // there is already a node there, so forward message to that node
            node ! Insert(requester, id, elemNew)
        }
      }


    case Remove(requester: ActorRef, id: Int, elemRemove: Int) =>

      if( elemRemove == elem ) {
        removed = true
        println(s"OperationFinished(${id}) (remove)")
        requester ! OperationFinished(id)
      }
      else {
        val pos: Position = if (elemRemove > elem) Right else Left
        subtrees.get(pos) match {
          case None => requester ! OperationFinished(id) // notify requester
          case Some(node) => node ! Remove(requester, id, elemRemove)
        }
      }


    /**
      * Implement an internal CopyTo operation on the binary tree that copies
      * all its non-removed contents from the binary tree to a provided new one.
      * This implementation can assume that no operations arrive while the
      * copying happens (i.e. the tree is protected from modifications while
      * copying takes places).
      * */
    case CopyTo(root: ActorRef) =>

      if (removed && subtrees.isEmpty) sender ! CopyFinished
      else {
        // If not flagged as 'removed', copy node to the new tree
        if (!removed) root ! Insert(self, elem, elem) // make int value the id (elem)?
        // Copy the left and right child trees
        val leftRight = subtrees.values
        leftRight.foreach(tree => tree ! CopyTo(root))
        context.become(copying(leftRight.toSet, removed))
      }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        println("Operation finished  ........................................")
        context.become(normal)
      }
      else context.become(copying(expected, true))

    case CopyFinished =>
      val expNext: Set[ActorRef] = expected - sender
      if (expNext.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        println("Copy finished ........................................")
        context.become(normal)
      }
      else context.become(copying(expNext, insertConfirmed))

  }

}
