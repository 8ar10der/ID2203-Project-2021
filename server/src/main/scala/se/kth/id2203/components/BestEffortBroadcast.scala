package se.kth.id2203.components

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}

import scala.collection.immutable.Set

case class BEB_Deliver(source: NetAddress, event: KompicsEvent) extends KompicsEvent

case class BEB_Broadcast(payload: KompicsEvent, topology: Set[NetAddress]@unchecked) extends KompicsEvent;

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver]
  request[BEB_Broadcast]
}

class BasicBroadcast extends ComponentDefinition {

  //BasicBroadcast Subscriptions

  val net = requires[Network]
  val beb = provides[BestEffortBroadcast]

  //BasicBroadcast Component State and Initialization
  val self = cfg.getValue[NetAddress]("id2203.project.address")

  //BasicBroadcast Event Handlers
  beb uponEvent {
    case x: BEB_Broadcast =>
      for (q <- x.topology) {
        trigger(
          NetMessage(self, q, x) -> net
        )
      }
  }

  net uponEvent {
    case NetMessage(src, BEB_Broadcast(payload, _)) =>
      trigger(
        BEB_Deliver(src.getSource(), payload) -> beb
      )
  }
}
