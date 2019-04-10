package kvstore

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorContext, ActorRef, Cancellable, OneForOneStrategy, Props, ReceiveTimeout}
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted, PersistenceException}
import kvstore.Replica._
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

  case class ReplicationActors(replica: ActorRef, replicationManager: ActorRef)

  case class Messages(messages:Set[Replicate])

  case class ReplicationManagementInfo(id: Long, replyTo: ActorRef, pendingReplications: Set[ReplicationActors]) {
    def removeReplicationManager(replicationManager: ActorRef): ReplicationManagementInfo = {
      copy(pendingReplications = pendingReplications.filterNot(r => r.replicationManager == replicationManager))
    }

    def stopAndRemoveReplicationManager(replicas: Set[ActorRef], context: ActorContext): ReplicationManagementInfo = {
      pendingReplications.filter(r => replicas.contains(r.replica)).foreach { ra =>
        context.stop(ra.replicationManager)
      }
      val removed = pendingReplications.filterNot(r => replicas.contains(r.replica))
      copy(pendingReplications = removed)
    }

    def isComplete: Boolean = pendingReplications.isEmpty
  }

  case class ReplicationManagerFinished(id: Long)

  case class ReplicationManagerFailed(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  def repManagerProps(ackId: Long, replyTo: ActorRef, replicator: ActorRef, messages: Set[Replicate]): Props =
    Props(new ReplicationManager(ackId, replyTo, replicator, messages))
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
  // a map from secondary replicas to replicator
  var secondaries = Map.empty[ActorRef, ActorRef]

  def secondaryReplicas = secondaries.map(_._1).toSet

  // the current set of synchReplicators
  var replicators = Set.empty[ActorRef]
  var _seqCounter = 0L

  var replicationIdCounter = 0L

  def nextRepId() = {
    val ret = replicationIdCounter
    replicationIdCounter += 1
    ret
  }

  // map operationId -> ReplicationManagementInfo(id: Long, replyTo: ActorRef, replica: ActorRef, replicationManager: ActorRef)
  // when new replicas received iterate all sets and remove any entries with matching remove replica if any end
  // when ReplicationDone received, use id and sender to find and remove entry for that ReplicationManager from pending
  // if pending becomes empty use replyto and id to operationAck
  var pendingReplications: Map[Long, ReplicationManagementInfo] = Map.empty

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
      context.become(persistingLeader(sender(), persist, cancellable))
    case Remove(key, id) =>
      kv -= (key)
      val persist = Persist(key, None, id)
      context.setReceiveTimeout(1000 milliseconds)
      val cancellable: Cancellable = context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, persistence, persist)
      context.become(persistingLeader(sender(), persist, cancellable))
    case Replicas(replicasIncPrimary) =>
      val replicas = replicasIncPrimary - self
      val removed = secondaryReplicas.diff(replicas)
      val newReplicasWithReplicators = replicas.diff(secondaryReplicas).map { rep =>
        (rep, context.actorOf(Replicator.props(rep)))
      }
      val remaining = secondaries.filterNot(t => removed.contains(t._1))
      secondaries = remaining ++ newReplicasWithReplicators
      replicators = secondaries.map(_._2).toSet
      val replicatorsToStop = secondaries.filter(t => removed.contains(t._1)).map(_._2).toSet
      stopReplicationManagers(replicas)
      stopRemovedReplicators(replicatorsToStop)
      synchReplicas(nextRepId(), sender(), replicateMessages, newReplicasWithReplicators.toMap)
    case ReplicationManagerFinished(id) =>
      val pending: ReplicationManagementInfo = pendingReplications(id)
      val updated = pending.removeReplicationManager(sender())
      if (updated.isComplete) {
        updated.replyTo ! OperationAck(updated.id)
        pendingReplications -= id
      } else {
        pendingReplications += (id -> updated)
      }

    case _ =>
  }

  def stopReplicationManagers(removedReplicas: Set[ActorRef]) = {
    pendingReplications.foreach { case (id, info) =>
      info.stopAndRemoveReplicationManager(removedReplicas, context)
      if (info.isComplete) {
        info.replyTo ! OperationAck(info.id)
      }
    }
    pendingReplications = pendingReplications.filterNot(_._2.isComplete)
  }

  def replicateMessages = kv.map { case (key, value) => Replicate(key, Option(value), nextRepId()) }.toSet

  private def stopRemovedReplicators(removed: Set[ActorRef]) = {
    removed.foreach(context.stop)
  }

  def persistingLeader(replyTo: ActorRef, persist: Persist, cancellable: Cancellable): Receive = {
    case Persisted(_, id) =>
      cancellable.cancel()
      synchReplicas(id, replyTo, Set(Replicate(persist.key, persist.valueOption, id)), secondaries)
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case ReceiveTimeout =>
      cancellable.cancel()
      replyTo ! OperationFailed(persist.id)
      context.become(leader)
    case ReplicationManagerFinished(id) =>
      val pending: ReplicationManagementInfo = pendingReplications(id)
      val updated = pending.removeReplicationManager(sender())
      if (updated.isComplete) {
        updated.replyTo ! OperationAck(updated.id)
        pendingReplications -= id
      } else {
        pendingReplications += (id -> updated)
      }
  }


  def synchReplicas(id: Long, replyTo: ActorRef, replicate: Set[Replicate], synchReplicators: Map[ActorRef, ActorRef]): Unit = {
    if (synchReplicators.isEmpty) {
      replyTo ! OperationAck(id)
      context.become(leader)
    } else {
      val repManagerId = nextRepId()
      val replicationActors = synchReplicators.map { case (replica, replicator) =>
        val ref = context.actorOf(repManagerProps(repManagerId, self, replicator, replicate))
        ref ! Messages(replicate)
        ReplicationActors(replica, ref)
      }.toSet
      pendingReplications += (repManagerId -> ReplicationManagementInfo(repManagerId, replyTo, replicationActors))
    }
  }

  def persistingReplica(replyTo: ActorRef, persist: Persist, cancellable: Cancellable): Receive = {
    case Persisted(key, seq) =>
      cancellable.cancel()
      replyTo ! SnapshotAck(key, seq)
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

class ReplicationManager(ackId: Long, replyTo: ActorRef, replicator: ActorRef, messages: Set[Replicate]) extends Actor {

  import context.dispatcher

  var messageSchedulers: Map[Long, (Cancellable)] = Map.empty


  override def receive: Receive = {
    case Messages(messages) =>
      if (messages.isEmpty) {
        replyTo ! ReplicationManagerFinished(ackId)
        context.stop(self)
      } else {
        messageSchedulers = messages.map { message =>
          (message.id, context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, replicator, message))
        }.toMap
      }

    case Replicated(_, id) =>
      messageSchedulers.get(id).map(_.cancel())
      val pending = messageSchedulers - id
      //check for removed synchReplicators and remove from pending
      if (pending.isEmpty) {
        replyTo ! ReplicationManagerFinished(ackId)
        context.stop(self)
      }
    case OperationFailed(id) =>
      messageSchedulers.foreach { case (_, c) => c.cancel() }
      replyTo ! ReplicationManagerFailed(ackId)
      context.stop(self)
    case ReceiveTimeout =>
      replyTo ! ReplicationManagerFailed(ackId)
      context.stop(self)
    case _ =>
  }
}

