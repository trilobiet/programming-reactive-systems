package stringcount

import akka.actor.{Actor, ActorRef, Props, Terminated}

case class StartProcessFileMsg()

/**
  * Based on
  * https://www.toptal.com/scala/concurrency-and-fault-tolerance-made-easy-an-intro-to-akka
  * However, with dead watch
  *
  * @param filename
  */
class WordCounterActor(filename: String) extends Actor {

  private var running = false
  private var result = 0
  private var fileSender: Option[ActorRef] = None

  def receive = {
    case StartProcessFileMsg() => {
      if (running) {
        // println just used for example purposes;
        // Akka logger should be used instead
        println("Warning: duplicate start message received")
      } else {
        running = true
        fileSender = Some(sender) // save reference to process invoker
        import scala.io.Source._
        fromFile(filename).getLines.foreach { line =>
          val lineActor = context.actorOf(Props(new StringCounterActor()))
          context.watch(lineActor)
          lineActor ! ProcessStringMsg(line)
        }
      }
    }
    case StringProcessedMsg(words) =>
      result += words
      context.stop(sender) // got the answer, child is no longer needed
    case Terminated(_) => // sent by a child actor (StringCounterActor) when it is stopped
      // provide result to process invoker when all children have terminated
      if (context.children.size == 0) fileSender.map(_ ! result)
    case _ => println("message not recognized!")
  }
}