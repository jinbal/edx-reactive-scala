package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props, ReceiveTimeout}
import kvstore.Replica.OperationFailed

import scala.concurrent.duration._
import scala.language.postfixOps
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

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valOpt, id) =>
      val replyTo = sender()
      val seq = nextSeq()
      context.setReceiveTimeout(1000 milliseconds)
      val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, replica, Snapshot(key, valOpt, seq))
      context.become(replicating(replyTo, cancellable,seq,key))
    case _ =>
  }

  def replicating(replyTo: ActorRef ,cancellable: Cancellable,id: Long, key:String): Receive = {
    case SnapshotAck(key, seq) =>
      replyTo ! Replicated(key, seq)
      context.unbecome()
    case ReceiveTimeout =>
      replyTo ! OperationFailed(id)
      context.unbecome()
//    case OperationFailed(failedId) =>
//      replyTo ! OperationFailed(failedId)
//      context.unbecome()
    case _ =>
  }

}
