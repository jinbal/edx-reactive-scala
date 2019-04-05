package kvstore

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}

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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks: Map[Long, (ActorRef, Replicate)] = Map.empty
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
      val s = sender()
      val seq = nextSeq()
      val tuple = (s, Replicate(key, valOpt, id))
      acks += (seq -> tuple)
      context.setReceiveTimeout(100 milliseconds)
      replica ! Snapshot(key, valOpt, seq)
      context.become(replicating)
    case _ =>
  }

  def replicating: Receive = {
    case SnapshotAck(key, seq) =>
      val (actor, replicate) = acks(seq)
      context.setReceiveTimeout(Duration.Undefined)
      acks -= seq
      actor ! Replicated(replicate.key, replicate.id)
      context.unbecome()
    case ReceiveTimeout =>
      context.setReceiveTimeout(100 milliseconds)
      acks.foreach { case (seq, (_, replicate)) =>
        replica ! Snapshot(replicate.key, replicate.valueOption, seq)
      }
    case _ =>
  }

}
