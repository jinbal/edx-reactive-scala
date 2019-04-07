package kvstore

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, Props, ReceiveTimeout}
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted, PersistenceException}
import kvstore.Replica.ReplicationComplete
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

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

  case class ReplicationComplete(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  def replicatingProps(leader: ActorRef, replicator: ActorRef, replicates: Set[Replicate], replicatingId: Long): Props =
    Props(new Replicating(leader, replicator, replicates, replicatingId))
}

class Replicating(leader: ActorRef, replicator: ActorRef, replicates: Set[Replicate], replicatingId: Long) extends Actor {
  var pendingReplicates = replicates
  pendingReplicates.foreach { r => replicator ! r }

  override def receive: Receive = {
    case Replicated(key, id) =>
      pendingReplicates = pendingReplicates.dropWhile(r => r.id == id)
      if (pendingReplicates.isEmpty) {
        leader ! ReplicationComplete(replicatingId)
        context.stop(self)
      }
    case _ =>
  }
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
  context.watch(persistence)
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var _seqCounter = 0L
  // map(id -> (replica, replicatingactor))
  var pendingReplicaInit: Map[Long, (ActorRef, ActorRef)] = Map.empty

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

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
      persistFromLeader(persist)
    case Remove(key, id) =>
      kv -= (key)
      val persist = Persist(key, None, id)
      persistFromLeader(persist)
    case Replicas(replicas) =>
      val (remaining, removed) = secondaries.partition(r => replicas.contains(r._1))
      val newReps = replicas.filter(r => !remaining.contains(r)).map { rep =>
        (rep, context.actorOf(Replicator.props(rep)))
      }.toMap
      val fullyUpdated = remaining ++ newReps
      secondaries = fullyUpdated
      replicators = fullyUpdated.map(_._2).toSet
      val removedReplicas = removed.map(_._2).toSet
      stopRemovedReplicators(removedReplicas)
//      cancelRemovedRepPopulation(removedReplicas)
//      populateNewReplicas(newReps.toSet)
    case ReplicationComplete(id) =>
      pendingReplicaInit -= id
    case _ =>
  }

  private def cancelRemovedRepPopulation(removedReplicas: Set[ActorRef]) = {
    removedReplicas.foreach { removed =>
      val opt = pendingReplicaInit.find { case (_, (r, _)) => removed == r }
      opt match {
        case Some((id, (_, toStop))) =>
          context.stop(toStop)
          pendingReplicaInit -= id
        case None =>
      }
    }
  }

  private def populateNewReplicas(replicators: Set[(ActorRef, ActorRef)]) = {
    replicators.foreach { case (rep, replicator) =>
      val toReplicate = kvToReplicate
      val id = nextSeq()
      val repActor = context.actorOf(replicatingProps(self, replicator, toReplicate, id))
      val tuple = (rep, repActor)
      pendingReplicaInit += (id -> tuple)
    }
  }


  private def kvToReplicate: Set[Replicate] = {
    kv.map { case (key, value) =>
      Replicate(key, Option(value), nextSeq())
    }.toSet
  }

  private def stopRemovedReplicators(removed: Set[ActorRef]) = {
    removed.foreach(context.stop)
  }

  private def persistFromLeader(persist: Persist) = {
    context.setReceiveTimeout(1000 milliseconds)
    val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, persist)
    context.become(persistingLeader(sender(), persist, cancellable))
  }

  def persistingLeader(primaryReplica: ActorRef, persist: Persist, cancellable: Cancellable): Receive = {
    case Persisted(_, id) =>
      cancellable.cancel()
      synchReplicas(primaryReplica, persist)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case ReceiveTimeout =>
      cancellable.cancel()
      primaryReplica ! OperationFailed(persist.id)
      context.become(leader)
  }

  def synchReplicas(primaryReplica: ActorRef, persist: Persist): Unit = {
    val replicationMessages: Map[Long, Cancellable] = replicators.map { r =>
      val id = nextSeq()
      val message = Replicate(persist.key, persist.valueOption, id)
      val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, r, message)
      (id, cancellable)
    }.toMap

    context.become(waitForReplicaSynch(primaryReplica, replicationMessages, persist.id))
  }

  def waitForReplicaSynch(primaryReplica: ActorRef, messages: Map[Long, Cancellable], ackId: Long): Receive = {
    case Replicated(_, id) =>
      messages.get(id).map(_.cancel())
      if (messages.isEmpty) {
        primaryReplica ! OperationAck(ackId)
        context.become(leader)
      } else {
        val pending = messages - id
        context.become(waitForReplicaSynch(primaryReplica, pending, ackId))
      }
    case OperationFailed(id) =>
      messages.get(id).map(_.cancel())
      primaryReplica ! OperationFailed(ackId)
      context.become(leader)
    case _ =>
//      println(s"unexpected")
  }

  def persistingReplica(send: ActorRef, persist: Persist, cancellable: Cancellable): Receive = {
    case Persisted(key, seq) =>
      cancellable.cancel()
      send ! SnapshotAck(key, seq)
      context.become(replica)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
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
        val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, persist)
        context.become(persistingReplica(sender(), persist, cancellable))
      }
    case _ =>
  }
}

