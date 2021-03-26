package se.kth.id2203.components

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start, ComponentDefinition => _, Port => _}

class EventuallyPerfectFailureDetector extends Port {
    indication[Suspect]
    indication[Restore]
}

case class Suspect(src: Address) extends KompicsEvent

case class Restore(src: Address) extends KompicsEvent

case class HeartbeatReply(seq: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int) extends KompicsEvent;

//Define EPFD Implementation
class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

    //EPFD subscriptions
    val timer = requires[Timer]
    val net = requires[Network]
    val epfd = provides[EventuallyPerfectFailureDetector]

    // EPDF component state and initialization

    //configuration parameters
    val self = epfdInit match {case Init(s: NetAddress) => s}
    val topology = List.empty[NetAddress]
    val delta = cfg.getValue[Long]("id2203.project.delay")

    //mutable state
    var delay = cfg.getValue[Long]("id2203.project.delay")
    var alive = Set(List.empty[NetAddress]: _*)
    var suspected = Set[NetAddress]()
    var seqnum = 0

    def startTimer(delay: Long): Unit = {
        val scheduledTimeout = new ScheduleTimeout(delay)
        scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout))
        trigger(scheduledTimeout -> timer)
    }

    //EPFD event handlers
    ctrl uponEvent {
        case _: Start =>
            /* WRITE YOUR CODE HERE  */
            startTimer(delay)
    }

    timer uponEvent {
        case CheckTimeout(_) =>
            if (alive.intersect(suspected).nonEmpty) {

                /* WRITE YOUR CODE HERE  */
                delay += delay

            }

            seqnum = seqnum + 1

            for (p <- topology) {
                if (!alive.contains(p) && !suspected.contains(p)) {

                    /* WRITE YOUR CODE HERE  */
                    suspected = suspected + p
                    trigger(Restore(p) -> epfd)

                } else if (alive.contains(p) && suspected.contains(p)) {
                    suspected = suspected - p
                    trigger(Restore(p) -> epfd)
                }
                trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> net)
            }
            alive = Set[NetAddress]()
            startTimer(delay)
    }

    net uponEvent {
        case NetMessage(src, HeartbeatRequest(seq)) =>
            trigger(NetMessage(self, src.getSource(), HeartbeatReply(seq)) -> net)
        case NetMessage(src, HeartbeatReply(seq)) =>
            if (seq == seqnum && suspected.contains(src.getSource()))
                alive = alive + src.getSource()
    }
};
