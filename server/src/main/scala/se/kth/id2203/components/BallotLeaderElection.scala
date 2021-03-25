package se.kth.id2203.components

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}

import scala.collection.mutable

class BallotLeaderElection extends Port {
  indication[BLE_Leader]
  request[BLE_Set]
}

@SerialVersionUID(121314583133228661L)
case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent with Serializable
@SerialVersionUID(121314583133228661L)
case class BLE_Set(topology: Set[NetAddress]) extends KompicsEvent with Serializable

//Provided Primitives to use in your implementation
@SerialVersionUID(121314583133228661L)
case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout) with Serializable
@SerialVersionUID(121314583133228661L)
case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent with Serializable
@SerialVersionUID(121314583133228661L)
case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent with Serializable

class GossipLeaderElection() extends ComponentDefinition {

  private val ballotOne = 0X0100000000L
  def ballotFromNAddress(n: Int, adr: Address): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n)
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode())
    val bytes = nBytes ++ addrBytes
    val r = com.google.common.primitives.Longs.fromByteArray(bytes)
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

  val ble: NegativePort[BallotLeaderElection] = provides[BallotLeaderElection]
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")

  var topology: List[NetAddress] = List[NetAddress]().empty
  val delta: Long = cfg.getValue[Long]("id2203.project.delay")
  var majority: Int = 0

  private var period = cfg.getValue[Long]("id2203.project.delay")
  private val ballots = mutable.Map.empty[NetAddress, Long]

  private var round = 0L
  private var ballot = ballotFromNAddress(0, self)

  private var leader: Option[(Long, NetAddress)] = None
  private var highestBallot: Long = ballot

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period)
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout))
    trigger(scheduledTimeout -> timer)
  }

  private def makeLeader(topProcess: (Long, NetAddress)) {
    leader = Some(topProcess)
  }

  private def checkLeader() {
    var (topProcess, topBallot) = (self, ballot)
    //MaxByBallot
    for ((p, b) <- ballots) {
      if (b > topBallot) {
        topProcess = p
        topBallot = b
      }
    }
    if (topBallot < highestBallot) {
      while (ballot <= highestBallot) {
        ballot = incrementBallot(ballot)
      }
      leader = None
    } else {
      val top = (topBallot, topProcess)
      if (!leader.contains(top)) {
        highestBallot = topBallot
        leader = Some(top)
        trigger(
          BLE_Leader(topProcess, topBallot) -> ble
        )
      }
    }
  }

  ble uponEvent {
    case BLE_Set(t) =>
      topology = topology ::: t.toList
      majority = (topology.size / 2) + 1
      startTimer(period)
  }

  timer uponEvent {
    case CheckTimeout(_) =>
      if (ballots.size + 1 >= majority) {
        checkLeader()
      }
      ballots.clear
      round += 1
      for (p <- topology) {
        if (p != self) {
          trigger(
            NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> net
          )
        }
      }
      startTimer(period)
  }

  net uponEvent {
    case NetMessage(src, HeartbeatReq(r, hb)) =>
      if (hb > highestBallot) {
        highestBallot = hb
      }
      trigger(
        NetMessage(self, src.getSource(), HeartbeatResp(r, ballot)) -> net
      )
    case NetMessage(src, HeartbeatResp(r, b)) =>
      if (r == round) {
        ballots += (src.getSource() -> b)
      } else {
        period += delta
      }
  }
}
