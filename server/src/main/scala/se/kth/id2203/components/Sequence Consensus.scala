package se.kth.id2203.components

import se.kth.id2203.components
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network._
import se.sics.kompics.sl._

import scala.collection.mutable

class SequenceConsensus extends Port {
  request[SC_Propose]
  indication[SC_Decide]
}

case class SC_Propose(value: Operation) extends KompicsEvent

case class SC_Decide(value: Operation) extends KompicsEvent

trait Operation

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent

case class Promise(nL: Long, na: Long, suffix: List[Operation], ld: Int) extends KompicsEvent

case class AcceptSync(nL: Long, suffix: List[Operation], ld: Int) extends KompicsEvent

case class Accept(nL: Long, c: Operation) extends KompicsEvent

case class Accepted(nL: Long, m: Int) extends KompicsEvent

case class Decide(ld: Int, nL: Long) extends KompicsEvent

object State extends Enumeration {
  type State = Value
  val PREPARE, ACCEPT, UNKOWN = Value
}

object Role extends Enumeration {
  type Role = Value
  val LEADER, FOLLOWER = Value
}

class SequencePaxos() extends ComponentDefinition {

  import Role._
  import State._

  def suffix(s: List[Operation], l: Int): List[Operation] = {
    s.drop(l)
  }

  def prefix(s: List[Operation], l: Int): List[Operation] = {
    s.take(l)
  }

  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.OrderingOps = o.mkOrderingOps(x)

  val sc: NegativePort[SequenceConsensus] = provides[SequenceConsensus]
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection]
  val net: PositivePort[Network] = requires[Network]

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  val pi: Set[NetAddress] = Set[NetAddress]()
  val others: Set[NetAddress] = Set[NetAddress]()

  val majority: Int = (pi.size / 2) + 1

  var state: (components.Role.Value, components.State.Value) = (FOLLOWER, UNKOWN)
  var nL = 0L
  var nProm = 0L
  var leader: Option[Address] = None
  var na = 0L
  var va = List.empty[Operation]
  var ld = 0
  // leader state
  var propCmds = List.empty[Operation]
  val las = mutable.Map.empty[Address, Int]
  val lds = mutable.Map.empty[Address, Int]
  var lc = 0
  val acks = mutable.Map.empty[Address, (Long, List[Operation])]

  ble uponEvent {
    case BLE_Leader(l, n) =>
      if (n > nL) {
        leader = Some(l)
        nL = n
        if (self == l && nL > nProm) {
          state = (LEADER, PREPARE)
          propCmds = propCmds.empty
          las.clear
          lds.clear
          acks.clear
          lc = 0
          for (p <- others) {
            trigger(
              NetMessage(self, p, Prepare(nL, ld, na)) -> net
            )
          }
          acks += (l -> (na, suffix(va, ld)))
          lds += (self -> ld)
          nProm = nL
        } else {
          state = (FOLLOWER, state._2)
        }
      }
  }

  net uponEvent {
    case NetMessage(p, Prepare(np, _, n)) =>
      if (nProm < np) {
        nProm = np
        state = (FOLLOWER, PREPARE)
        if (na >= n) {
          trigger(
            NetMessage(self, p.getSource(), Promise(np, na, suffix(va, ld), ld)) -> net
          )
        } else {
          trigger(
            NetMessage(self, p.getSource(), Promise(np, na, List.empty[Operation], ld)) -> net
          )
        }
      }
    case NetMessage(aa, Promise(n, na, sfxa, lda)) =>
      val a = aa.getSource()
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += (a -> (na, sfxa))
        lds += (a -> lda)
        val P = pi.filter(acks.contains(_))
        if (P.size == majority) {
          var (k, sfx) = (0L, List.empty[Operation])
          //MAX
          for (p <- P) {
            val Some((kp, sfxp)) = acks.get(p)
            if (kp > k) {
              k = kp
              sfx = sfxp
            }
          }
          va = prefix(va, ld) ++ sfx ++ propCmds
          las += (self -> va.size)
          propCmds = propCmds.empty
          state = (LEADER, ACCEPT)
          for (p <- pi.filter(lds.contains(_))) {
            if (p != self) {
              val sfxp = suffix(va, lds(p))
              trigger(
                NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> net
              )
            }
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds += (a -> lda)
        val sfx = suffix(va, lds(a))
        trigger(
          NetMessage(self, a, AcceptSync(nL, sfx, lds(a))) -> net
        )
        if (lc != 0) {
          trigger(
            NetMessage(self, a, Decide(ld, nL)) -> net
          )
        }
      }
    case NetMessage(pp, AcceptSync(nL, sfxv, ld)) =>
      val p = pp.getSource()
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL
        va = prefix(va, ld) ++ sfxv
        trigger(
          NetMessage(self, p, Accepted(nL, va.size)) -> net
        )
        state = (FOLLOWER, ACCEPT)
      }
    case NetMessage(p, Accept(nL, c)) =>
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va = c +: va
        trigger(
          NetMessage(self, p.getSource(), Accepted(nL, va.size)) -> net
        )
      }
    case NetMessage(_, Decide(l, nL)) =>
      if (nProm == nL) {
        while (ld < l) {
          trigger(
            SC_Decide(va(ld)) -> sc
          )
          ld += 1
        }
      }
    case NetMessage(a, Accepted(n, m)) =>
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las += (a.getSource() -> m)
        if (lc < m && pi.count(las.get(_) >= m) >= majority) {
          lc = m
          for (p <- pi.filter(lds.contains(_))) {
            trigger(
              NetMessage(self, p, Decide(lc, nL)) -> net
            )
          }
        }
      }
  }

  sc uponEvent {
    case SC_Propose(c) =>
      if (state == (LEADER, PREPARE)) {
        propCmds = c +: propCmds
      } else if (state == (LEADER, ACCEPT)) {
        va = c +: va
        las += (self -> 1)
        for (p <- pi.filter(lds.contains(_))) {
          if (p != self) {
            trigger(
              NetMessage(self, p, Accept(nL, c)) -> net
            )
          }
        }
      }
  }
}
