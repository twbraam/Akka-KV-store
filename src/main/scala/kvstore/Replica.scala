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
      val persisted = (persistence ? Persist(key, Some(value), id)).mapTo[Persisted]
      tryingToPersist += (id -> (sender, Persist(key, Some(value), id)))
      persisted.pipeTo(self)

    case Remove(key: String, id: Long) =>
      kv -= key
      implicit val timeout: Timeout = Timeout(100.millis)
      val persisted = (persistence ? Persist(key, None, id)).mapTo[Persisted]
      tryingToPersist += (id -> (sender, Persist(key, None, id)))
      persisted.pipeTo(self)

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
          restarts = tryingToPersist.foreach(x => )
      }
  }

  def persist_yo()

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
        implicit val timeout: Timeout = Timeout(100.millis)
        val persist = (persistence ? Persist(key, valueOption, seq)).mapTo[Persisted]

        afterPersisted += (seq -> sender)
        persist.pipeTo(self)
      }
      else if (persisted.contains(seq)) sender ! SnapshotAck(key, seq)
    case Persisted(key: String, seq: Long) =>
      persisted += seq
      log.info(s"Persisted: key: $key, seq: $seq")
      afterPersisted(seq) ! SnapshotAck(key, seq)
      afterPersisted -= seq

    case Failure(ex) =>
      ex match {
        case _: AskTimeoutException =>
          log.info(s"timeout received")
          acks.foreach { x => self ! x._2._2 }
      }
  }
}

