package kvstore

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class ReplicateRetry(key: String, valueOption: Option[String], seq: Long)
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

  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq(): Long = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      val seq = nextSeq()
      replica ! Snapshot(key, valueOption, seq)
      acks += ((seq, (sender, Replicate(key, valueOption, id))))
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! ReplicateRetry(key, valueOption, seq)
      }

    case ReplicateRetry(key, valueOption, seq) if acks get seq nonEmpty =>
      replica ! Snapshot(key, valueOption, seq)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! ReplicateRetry(key, valueOption, seq)
      }

    case SnapshotAck(_, seq) if acks get seq nonEmpty =>
      for ((primary, Replicate(key, _, id)) <- acks get seq) {
        acks -= seq
        primary ! Replicated(key, id)
      }
  }
}