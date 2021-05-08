package stringcount

import akka.actor.Actor

case class ProcessStringMsg(string: String)
case class StringProcessedMsg(words: Integer)

class StringCounterActor extends Actor {
  def receive = {
    case ProcessStringMsg(string) => {
      val wordsInLine = string.split(" ").length
      context.parent ! StringProcessedMsg(wordsInLine)
    }
    case _ => println("Error: message not recognized")
  }
}