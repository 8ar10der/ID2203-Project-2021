 /*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvstore

import se.kth.id2203.components.{SC_Decide, SC_Propose, SequenceConsensus}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.network.Network
import se.sics.kompics.sl._

import java.util.UUID
import scala.collection.mutable

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network]
  val route: PositivePort[Routing.type] = requires(Routing)
  val sc: PositivePort[SequenceConsensus] = requires[SequenceConsensus]
  //******* Fields ******
  val storePart: mutable.HashMap[String, String] = mutable.HashMap("1" -> "a", "2" -> "ab", "10" -> "abcde")
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  val opSrcs: mutable.Map[UUID, NetAddress] = mutable.HashMap().empty
  //******* Handlers ******
  net uponEvent{
    case NetMessage(header, op: Operation) =>
      log.debug(s"++++++++++++++++++++++++++++++++Got $op from Client<${header.getSource()}>")
      opSrcs += (op.id -> header.getSource())
      trigger(
        SC_Propose(op) -> sc
      )
  }


  sc uponEvent {
    case SC_Decide(o: Operation) =>
      log.info("-------------------------------------------------------------------------------")
      val src: NetAddress = opSrcs.getOrElse(o.id, self)
      o match {
        case Get(key, _) =>
          val op = o.asInstanceOf[Get]
          log.info(s"Get Command from Client<${src}>")
          val value = storePart.get(key)
          if (value.isDefined) {
            trigger(
              NetMessage(self, src, op.response(OpCode.Ok, value.get)) -> net
            )
          } else {
            trigger(
              NetMessage(self, src, op.response(OpCode.NotFound)) -> net
            )
          }
        case Put(key, value, _) =>
          val op = o.asInstanceOf[Put]
          log.info(s"Put Command from Client<${src}>")
          storePart += (key -> value)
          trigger(
            NetMessage(self, src, op.response(OpCode.Ok)) -> net
          )
        case Cas(key, referenceValue, newValue, _) =>
          val op = o.asInstanceOf[Cas]
          log.info(s"Cas Command from Client<${src}>")
          if (storePart.contains(key)) {
            val value = storePart(key)
            if (value == referenceValue) {
              storePart += (key -> newValue)
              trigger(
                NetMessage(self, src, op.response(OpCode.Ok)) -> net
              )
            } else {
              trigger(
                NetMessage(self, src, op.response(OpCode.NotSwap)) -> net
              )
            }
          } else {
            trigger(
              NetMessage(self, src, op.response(OpCode.NotFound)) -> net
            )
          }
      }
  }
}
