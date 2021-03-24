//Rremember to execute the following imports first
import se.kth.edx.id2203.core.ExercisePrimitives._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{ComponentDefinition => _, Port => _,KompicsEvent}

import scala.collection.mutable.Map
import scala.language.implicitConversions

class AtomicRegister extends Port {
	request[AR_Read_Request]
	request[AR_Write_Request]
	indication[AR_Read_Response]
	indication[AR_Write_Response]
}

case class AR_Read_Request() extends KompicsEvent
case class AR_Read_Response(value: Option[Any]) extends KompicsEvent
case class AR_Write_Request(value: Any) extends KompicsEvent
case class AR_Write_Response() extends KompicsEvent

//The following events are to be used internally by the Atomic Register implementation below
case class READ(rid: Int) extends KompicsEvent;
case class VALUE(rid: Int, ts: Int, wr: Int, value: Option[Any]) extends KompicsEvent;
case class WRITE(rid: Int, ts: Int, wr: Int, writeVal: Option[Any]) extends KompicsEvent;
case class ACK(rid: Int) extends KompicsEvent;

/**
* This augments tuples with comparison operators implicitly, which you can use in your code. 
* examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true' 
*/
implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.OrderingOps = o.mkOrderingOps(x);

//HINT: After you execute the latter implicit ordering you can compare tuples as such within your component implementation:
(1,2) <= (1,4);

class ReadImposeWriteConsultMajority(init: Init[ReadImposeWriteConsultMajority]) extends ComponentDefinition {

  //subscriptions

  val nnar = provides[AtomicRegister];

  val pLink = requires[PerfectLink];
  val beb = requires[BestEffortBroadcast];

  //state and initialization

  val (self: Address, n: Int, selfRank: Int) = init match {
    case Init(selfAddr: Address, n: Int) => (selfAddr, n, AddressUtils.toRank(selfAddr))
  };

  var (ts, wr) = (0, 0);
  var value: Option[Any] = None;
  var acks = 0;
  var readval: Option[Any] = None;
  var writeval: Option[Any] = None;
  var rid = 0;
  var readlist: Map[Address, (Int, Int, Option[Any])] = Map.empty
  var reading = false;

  //handlers

  nnar uponEvent {
    case AR_Read_Request() => {
      rid = rid + 1;
      acks += 1
      readlist = Map.empty
      reading = true
      trigger(
          BEB_Broadcast(READ(rid)) -> beb
          )
     
    };
    case AR_Write_Request(wval) => { 
      rid = rid + 1;
      writeval = Some(wval)
      acks = 0
      readlist = Map.empty
      trigger(
          BEB_Broadcast(READ(rid)) -> beb
          )    
     
    }
  }

  beb uponEvent {
    case BEB_Deliver(src, READ(readID)) => {
        
      trigger(
          PL_Send(src, VALUE(readID,ts,wr,value)) -> pLink
          )
     
    }
    case BEB_Deliver(src, w: WRITE) => {
     
     val WRITE(rx,tsx,wrx,vx) = w  
     if ((tsx,wrx) > (ts,wr)){
         ts = tsx
         wr = wrx
         value = vx
     }
     
     trigger(
         PL_Send(src,ACK(rx)) -> pLink
         )
     
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, v: VALUE) => {
      if (v.rid == rid) {
         
        readlist.put(src,(v.ts,v.wr,v.value))
        if(readlist.size > n/2){
            var maxts = 0
            var rr = 0
            readval = Some(0)
            var bcastval : Option[Any] = None
            var q = self
            //HIGHEST
            for((ad,(a,b,c)) <- readlist){
                if ((maxts,rr) <= (a,b)){
                    maxts = a
                    rr = b
                    readval = c
                    q = ad
                }
            }
            // readlist = readlist.-(q)
            readlist.clear
            if(reading)
                bcastval = readval
            else{
                rr = selfRank
                maxts = maxts + 1
                bcastval = writeval
            }
            trigger(
                BEB_Broadcast(WRITE(rid,maxts,rr,bcastval)) -> beb
                )
        }
      }
    }
    case PL_Deliver(src, v: ACK) => {
      if (v.rid == rid) {
        acks += 1
        if (acks > n/2) {
            acks = 0
            if (reading) {
                reading = true
                trigger(
                     AR_Read_Response(readval) -> nnar
                    )
            } else{
                trigger(
                     AR_Write_Response() -> nnar
                    )
            }
        }
     
      }
    }
  }
}
