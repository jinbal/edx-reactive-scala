/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
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


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation =>
      root.forward(operation)
    case GC =>
      //      println(s"GC started for root ${root.toString()} this ${self.toString()}")
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }


  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      //      println("GC done")
      pendingQueue.foreach {
        newRoot ! _
      }
      pendingQueue = Queue.empty
      root = newRoot
      context.unbecome()
    case operation: Operation =>
      pendingQueue = pendingQueue.enqueue(operation)
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case insert: Insert =>
      if (insert.elem == elem && !initiallyRemoved) {
        removed = false
        insert.requester ! OperationFinished(insert.id)
      } else {
        val position = getPosition(insert)
        if (subtrees.contains(position)) {
          subtrees(position) ! insert
        } else {
          newChild(position, insert)
          insert.requester ! OperationFinished(insert.id)
        }
      }
    case remove: Remove =>
      if (remove.elem == elem && !initiallyRemoved) {
        removed = true
        remove.requester ! OperationFinished(remove.id)
      } else {
        val position = getPosition(remove)
        if (subtrees.contains(position)) {
          subtrees(position) ! remove
        } else {
          remove.requester ! OperationFinished(remove.id)
        }
      }

    case contains: Contains =>
      if (elem == contains.elem && !initiallyRemoved) {
        contains.requester ! ContainsResult(contains.id, !removed)
      } else {
        val position = getPosition(contains)
        if (!subtrees.contains(position)) {
          contains.requester ! ContainsResult(contains.id, false)
        } else {
          subtrees(position) ! contains
        }
      }
    case CopyTo(newRoot) =>
      if (!removed) {
        newRoot ! Insert(self, -1, elem)
      }
      val childActors: Set[ActorRef] = subtrees.values.toSet
      childActors.foreach { a =>
        a ! CopyTo(newRoot)
      }
      isCopyDone(childActors, removed)


  }

  private def getPosition(operation: Operation) = {
    if (operation.elem < elem) Left else Right
  }

  private def newChild(position: Position, insert: Insert) = {
    val actor = context.actorOf(props(insert.elem, false))
    subtrees += (position -> actor)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(-1) => isCopyDone(expected, true)
    case CopyFinished => isCopyDone(expected - sender, insertConfirmed)
  }

  def isCopyDone(expected: Set[ActorRef], insertConfirmed: Boolean): Unit = {
    if (expected.isEmpty && insertConfirmed) self ! PoisonPill
    else context.become(copying(expected, insertConfirmed))
  }

  override def postStop() = context.parent ! CopyFinished

}

