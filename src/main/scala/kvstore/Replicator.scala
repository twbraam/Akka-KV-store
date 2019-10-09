package kvstore

import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, ReceiveTimeout}
import akka.event.LoggingReceive

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import kvstore.Persistence.PersistenceException
object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  implicit val timeout: Timeout = Timeout(100.millis)
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case Replicate(key: String, valueOption: Option[String], id: Long) =>
      log.info(s"replicate received: $key, $id")
      val value = (sender, Replicate(key, valueOption, id))
      acks += (id -> value)
      nextSeq()

      val x = (replica ? Snapshot(key, valueOption, id)).mapTo[SnapshotAck]
      x.pipeTo(self)

    case Failure(ex) =>
      ex match {
        case e: PersistenceException =>
          log.info(s"timeout received")
          acks.foreach { x => self ! x._2._2 }
      }

    case SnapshotAck(key: String, seq: Long) =>
      log.info(s"acknowledgement received: $key, $seq")
      val orig_sender = acks(seq)._1
      orig_sender ! Replicated(key, seq)
      acks -= seq
  }

}
