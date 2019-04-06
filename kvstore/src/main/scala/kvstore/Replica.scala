package kvstore

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, Props, ReceiveTimeout, Terminated}
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted, PersistenceException}
import kvstore.Replicator.{Snapshot, SnapshotAck}

import scala.concurrent.duration._

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import context.dispatcher
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 100, withinTimeRange = 1 second) {
      case _: PersistenceException => Resume
    }
  val persistence = context.actorOf(persistenceProps)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var _seqCounter = 0L

  arbiter ! Join

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Insert(key, value, id) =>
      kv += (key -> value)
      val persist = Persist(key, Some(value), id)
      context.setReceiveTimeout(1000 milliseconds)
      val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, persist)
      context.become(persistingLeader(sender(), persist,cancellable))
    case Remove(key, id) =>
      kv -= (key)
      val persist = Persist(key, None, id)
      context.setReceiveTimeout(1000 milliseconds)
      val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, persist)
      context.become(persistingLeader(sender(), persist, cancellable))
    case _ =>
  }

  def persistingLeader(send: ActorRef, persist: Persist, cancellable: Cancellable): Receive = {
    case Persisted(_, id) =>
      cancellable.cancel()
      send ! OperationAck(id)
      context.unwatch(persistence)
      context.become(leader)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case ReceiveTimeout =>
      cancellable.cancel()
      send ! OperationFailed(persist.id)
      context.unwatch(persistence)
      context.become(leader)
  }

  def persistingReplica(send: ActorRef, persist: Persist): Receive = {
    case Persisted(key, seq) =>
      send ! SnapshotAck(key, seq)
      context.become(replica)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case ReceiveTimeout =>
      context.setReceiveTimeout(100 milliseconds)
      persistence ! persist
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (seq <= _seqCounter) {
        if (seq == _seqCounter) {
          valueOption match {
            case Some(v) => kv += (key -> v)
            case None => kv -= (key)
          }
          _seqCounter = seq + 1
        }
        val persist = Persist(key, valueOption, seq)
        context.setReceiveTimeout(100 milliseconds)
        persistence ! persist
        context.become(persistingReplica(sender(), persist))
      }
    case _ =>
  }
}

