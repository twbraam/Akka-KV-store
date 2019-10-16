package kvstore

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.concurrent.duration._
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case class OperationStatus(id: Long, client: ActorRef)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long)
    extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props =
    Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props)
  extends Actor {
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  var persistence: ActorRef = _

  var kv = Map.empty[String, (String, Long)]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var replicationAcks = Map.empty[Long, (String, Option[ActorRef], Int)]
  var persistenceAcks = Map.empty[Long, ActorRef]

  var _replicatorId = 0L
  def nextReplicatorId(): Long = {
    val ret = _replicatorId
    _replicatorId += 1
    ret
  }

  def replicate(op: Operation,
                replicators: Set[ActorRef],
                currentReplicationAcks: Map[Long, (String, Option[ActorRef], Int)]):
  Map[Long, (String, Option[ActorRef], Int)] = {
    val msg = op match {
      case Insert(key, value, id) => Replicate(key, Some(value), id)
      case Remove(key, id)        => Replicate(key, None, id)
    }
    replicators foreach { x =>
      x ! msg
    }
    if (replicators.nonEmpty) currentReplicationAcks + ((op.id,
      (op.key, Some(sender), replicators.size))) else currentReplicationAcks
  }

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
    case t =>
      super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }

  override def preStart(): Unit = {
    arbiter ! Join
    persistence = context.actorOf(persistenceProps, "persistence")
  }

  def receive: PartialFunction[Any, Unit] = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(rep(0))
  }


  def leader: Receive = {
    case Insert(key, value, id) =>
      kv += ((key, (value, id)))

      val updatedReplicationAcks =
        replicate(Insert(key, value, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks

      persistence ! Persist(key, Some(value), id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! PersistRetry(key, Some(value), id)
      }
      persistenceAcks += id -> sender
      val client = sender
      context.system.scheduler.scheduleOnce(1 second) {
        self ! OperationStatus(id, client)
      }

    case Remove(key, id) =>
      kv -= key
      val updatedReplicationAcks =
        replicate(Remove(key, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks
      persistence ! Persist(key, None, id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! PersistRetry(key, None, id)
      }
      persistenceAcks += id -> sender
      val client = sender
      context.system.scheduler.scheduleOnce(1 second) {
        self ! OperationStatus(id, client)
      }

    case Get(key, id) =>
      val operationReply = kv get key match {
        case Some((value, _)) => GetResult(key, Some(value), id)
        case None             => GetResult(key, None, id)
      }
      sender ! operationReply

    case Replicas(replicas) =>
      val newReplicas = (replicas &~ secondaries.keySet) - self

      val removeReplicas = secondaries.keySet &~ replicas

      val replicatorsToKill = for {
        rep <- removeReplicas
        replicator <- secondaries.get(rep)
      } yield replicator

      replicatorsToKill.foreach(context.stop)
      for {
        (id, (key, _, _)) <- replicationAcks
        _ <- replicatorsToKill
      } self ! Replicated(key, id)
      val newReplicators = for (rep <- newReplicas) yield {
        val replicator = context.actorOf(Replicator.props(rep),
          "replicator-" + nextReplicatorId())
        val newReplicationAcks = for ((key, (value, id)) <- kv) yield {
          val newValue = replicationAcks get id match {
            case Some((_, requester, missingAcks)) =>
              (id, (key, requester, missingAcks + 1))
            case None if persistenceAcks get id nonEmpty =>
              (id, (key, Some(persistenceAcks(id)), 1))
            case None if persistenceAcks get id isEmpty =>
              (id, (key, None, 1))
          }
          replicator ! Replicate(key, Some(value), id)
          newValue
        }
        replicationAcks ++= newReplicationAcks
        replicator
      }
      replicators --= replicatorsToKill
      replicators ++= newReplicators
      secondaries --= removeReplicas
      secondaries ++= newReplicas zip newReplicators

    case Replicated(_, id) if persistenceAcks get id isEmpty =>
      replicationAcks(id) match {
        case (_, Some(requester), 1) =>
          requester ! OperationAck(id)
          replicationAcks -= id
        case (key, Some(requester), missingAcks) =>
          replicationAcks += ((id, (key, Some(requester), missingAcks - 1)))
        case (_, None, 1) =>
          replicationAcks -= id
        case (key, None, missingAcks) =>
          replicationAcks += ((id, (key, None, missingAcks - 1)))
      }

    case Replicated(_, id) if persistenceAcks get id nonEmpty =>
      replicationAcks(id) match {
        case (_, _, 1) => replicationAcks -= id
        case (key, requester, missingAcks) =>
          replicationAcks += ((id, (key, requester, missingAcks - 1)))
      }

    case PersistRetry(key, valueOption, id)
      if persistenceAcks get id nonEmpty =>
      persistence ! Persist(key, valueOption, id)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, valueOption, id)
      }

    case Persisted(_, id) if replicationAcks get id isEmpty =>

      persistenceAcks(id) ! OperationAck(id)
      persistenceAcks -= id

    case Persisted(_, id) if replicationAcks get id nonEmpty =>
      persistenceAcks -= id

    case OperationStatus(id, client)
      if (replicationAcks get id nonEmpty) || (persistenceAcks get id nonEmpty) =>
      client ! OperationFailed(id)
  }


  def rep(expectedSeq: Long): Receive = {
    case Insert(_, _, id) =>
      sender ! OperationFailed(id)

    case Remove(_, id) =>
      sender ! OperationFailed(id)

    case Get(key, id) =>
      val operationReply = kv get key match {
        case Some((value, _)) => GetResult(key, Some(value), id)
        case None             => GetResult(key, None, id)
      }
      sender ! operationReply

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)

    case Snapshot(key, Some(value), seq) if seq == expectedSeq =>
      kv += ((key, (value, seq)))
      persistence ! Persist(key, Some(value), seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, Some(value), seq)
      }
      persistenceAcks += seq -> sender

    case Snapshot(key, None, seq) if seq == expectedSeq =>
      kv -= key
      persistence ! Persist(key, None, seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, None, seq)
      }
      persistenceAcks += seq -> sender

    case PersistRetry(key, valueOption, seq)
      if persistenceAcks get seq nonEmpty =>
      persistence ! Persist(key, valueOption, seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, valueOption, seq)
      }

    case Persisted(key, seq) =>
      val requester = persistenceAcks(seq)
      persistenceAcks -= seq
      requester ! SnapshotAck(key, seq)
      context.become(rep(expectedSeq + 1))
  }
}