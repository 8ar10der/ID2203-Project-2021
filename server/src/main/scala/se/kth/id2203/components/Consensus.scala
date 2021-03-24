import se.kth.id2203.components.BestEffortBroadcast
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}

import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

 class Consensus extends Port{
   request[C_Propose];
   indication[C_Decide];
 }
 
 case class C_Decide(value: Any) extends KompicsEvent;
 case class C_Propose(value: Any) extends KompicsEvent;
 
  case class Prepare(proposalBallot: (Int, Int)) extends KompicsEvent;
  case class Promise(promiseBallot: (Int, Int), acceptedBallot: (Int, Int), acceptedValue: Option[Any]) extends KompicsEvent;
  case class Accept(acceptBallot: (Int, Int), proposedValue: Any) extends KompicsEvent;
  case class Accepted(acceptedBallot: (Int, Int)) extends KompicsEvent;
  case class Nack(ballot: (Int, Int)) extends KompicsEvent;
  case class Decided(decidedValue: Any) extends KompicsEvent;

  /**
    * This augments tuples with comparison operators implicitly, which you can use in your code, for convenience. 
    * examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true' 
    */
  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.OrderingOps = o.mkOrderingOps(x);
  
  //HINT: After you execute the latter implicit ordering you can compare tuples as such within your component implementation:
  (1,2) <= (1,4);
  
class Paxos(paxosInit: Init[Paxos]) extends ComponentDefinition {

  //Port Subscriptions for Paxos

  val consensus = provides[Consensus];
  val beb = requires[BestEffortBroadcast];
  val plink = requires[PerfectLink];
 
  //Internal State of Paxos
  val (rank, numProcesses) = paxosInit match {
    case Init(s: Address, qSize: Int) => (toRank(s), qSize)
  }

  //Proposer State
  var round = 0;
  var proposedValue: Option[Any] = None;
  var promises: ListBuffer[((Int, Int), Option[Any])] = ListBuffer.empty;
  var numOfAccepts = 0;
  var decided = false;

  //Acceptor State
  var promisedBallot = (0, 0);
  var acceptedBallot = (0, 0);
  var acceptedValue: Option[Any] = None;

  def propose() = {
    if (!decided) {
        round += 1
        numOfAccepts = 0
        promises.clear()
        trigger(
            BEB_Broadcast(Prepare((round,rank))) -> beb
            )
        
    }
  }

  consensus uponEvent {
    case C_Propose(value) => {
        proposedValue = Some(value)
        propose()
    }
  }


  beb uponEvent {

    case BEB_Deliver(src, prep: Prepare) => {
        val ballot = prep.proposalBallot
        if (ballot > promisedBallot) {
            promisedBallot = ballot
            trigger(
                PL_Send(src, Promise(promisedBallot,acceptedBallot,acceptedValue)) -> plink
                )
        } else {
            trigger(
                PL_Send(src, Nack(ballot)) -> plink
                )
        }
    };

    case BEB_Deliver(src, acc: Accept) => {
        val ballot = acc.acceptBallot
        val v = acc.proposedValue
        
        if (promisedBallot <= ballot) {
            promisedBallot = ballot
            acceptedBallot = ballot
            acceptedValue = Some(v)
            trigger(
                PL_Send(src, Accepted(ballot)) -> plink
                )
        } else {
            trigger(
                PL_Send(src, Nack(ballot)) -> plink
                )
        }
    };

    case BEB_Deliver(src, dec : Decided) => {
        if (!decided) {
            trigger(
                C_Decide(dec.decidedValue) -> consensus
                )
            decided = true
        }
    }
  }

  plink uponEvent {

    case PL_Deliver(src, prepAck: Promise) => {
      if ((round, rank) == prepAck.promiseBallot) {
        promises += ((prepAck.acceptedBallot, prepAck.acceptedValue))
        if(promises.size > (numProcesses+1)/2) {
            //HighestByBallot
            var maxBallot = (0, 0)
            var value: Option[Any] = None
            for(p <- promises){
                
                val (b,v) = p
                if (b > maxBallot){
                    maxBallot = b 
                    value = v
                }
            }
            
            if(value.isDefined){
                proposedValue = value
            }
            
            trigger(
                BEB_Broadcast(Accept((round,rank),proposedValue)) -> beb
                )
        }
      }
    };

    case PL_Deliver(src, accAck: Accepted) => {
      if ((round, rank) == accAck.acceptedBallot) {
        numOfAccepts += 1
        if (numOfAccepts > (numProcesses+1)/2) {
            trigger(
                BEB_Broadcast(Decided(proposedValue)) -> beb
                )
        }
      }
    };

    case PL_Deliver(src, nack: Nack) => {
      if ((round, rank) == nack.ballot) {
        propose()
      }
    }
  }
};
