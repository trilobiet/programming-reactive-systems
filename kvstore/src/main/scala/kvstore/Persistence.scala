package kvstore

import akka.actor.{Props, Actor}
import scala.util.Random

object Persistence {
  case class Persist(key: String, valueOption: Option[String], id: Long)
  case class Persisted(key: String, id: Long)

  class PersistenceException extends Exception("Persistence failure")

  def props(flaky: Boolean): Props = Props(classOf[Persistence], flaky)
}

class Persistence(flaky: Boolean) extends Actor {
  import Persistence._

  def receive = {
    case Persist(key, _, id) =>
      println(s"Persist $key,$id ")
      if (!flaky || Random.nextBoolean()) sender ! Persisted(key, id)
      // if (false || Random.nextBoolean()) sender ! Persisted(key, id) // force flaky
      else throw new PersistenceException
  }

}
