package kvstore

import akka.actor.{Actor, ActorKilledException, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import kvstore.Arbiter._
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy._

import scala.concurrent.duration._
import akka.util.Timeout

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var tryingToPersist = Map.empty[Long, (ActorRef, Persist)]
  var persisted = Set.empty[Long]


  var mySeq: Long = 0L
  val persistence: ActorRef = context.actorOf(persistenceProps)

  arbiter ! Join
  def receive: PartialFunction[Any, Unit] = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  implicit val timeout: Timeout = Timeout(900.millis)
  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {

    case Insert(key: String, value: String, id: Long) =>
      kv += (key -> value)
      val persist = Persist(key, Some(value), id)
      persist_yo(persist)
      val x = sender()
      tryingToPersist += (id -> (x, persist))

    case Remove(key: String, id: Long) =>
      kv -= key
      val persist = Persist(key, None, id)
      persist_yo(persist)
      val x = sender()
      tryingToPersist += (id -> (x, persist))

    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas: Set[ActorRef]) =>
      val newReps: Set[ActorRef] = replicas diff secondaries.keySet - self
      val newRep: ActorRef = newReps.head
      val newReplicator = context.actorOf(Replicator.props(newRep))
      secondaries += (newRep -> newReplicator)
      replicators += newReplicator

      kv.zipWithIndex.foreach{ case ((k: String, v: String), n: Int) => newReplicator ! Replicate(k, Some(v), n.toLong) }

    case Persisted(key: String, id: Long) =>
      tryingToPersist(id)._1 ! OperationAck(id)
      tryingToPersist -= id
  }

  var restarts: Int = 0
  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: PersistenceException =>
      restarts match {
        case toomany if toomany > 10 =>
          restarts = 0; Restart
        case n =>
          restarts += 1
          tryingToPersist.values.foreach(x => persist_yo(x._2))
          Resume
      }
  }

  def persist_yo(persist: Persist): Unit = {
    implicit val timeout: Timeout = Timeout(100.millis)
    val persistTry = (persistence ? persist).mapTo[Persisted]
    persistTry.pipeTo(self)

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key: String, id: Long) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key: String, valueOption: Option[String], seq: Long) =>
      log.info(s"Snapshot: key: $key, value: $valueOption, seq: $seq")

      if (seq == mySeq) { valueOption match {
        case Some(v) =>
          kv += (key -> v)
        case None =>
          kv -= key
      }
        mySeq += 1
        val persist: Persist = Persist(key, valueOption, seq)
        persist_yo(persist)
        val x = sender()
        tryingToPersist += (seq -> (x, persist))
      }
      else if (persisted.contains(seq)) sender ! SnapshotAck(key, seq)
    case Persisted(key: String, seq: Long) =>
      persisted += seq
      log.info(s"Persisted: key: $key, seq: $seq")
      tryingToPersist(seq)._1 ! SnapshotAck(key, seq)
      tryingToPersist -= seq
  }
}

