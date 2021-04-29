package actorbintree

import akka.actor.{Actor, ActorSystem, Props}

/**
  * https://alvinalexander.com/scala/simple-scala-akka-actor-examples-hello-world-actors/
  * @param name
  */
class HelloActor(name:String) extends Actor {

  override def receive = {
    case "hello" => println(s"hello back at you, ${name}")
    case "gutentag" => println(s"Gutentag, ${name}")
    case _       => println("huh?")
  }
}

object Main extends App {

  //val props = Props[HelloActor]
  val props = Props(new HelloActor("Richard"))
  println(s"Props: ${props}")

  val as = ActorSystem("You_name_it")
  // default Actor constructor
  val helloActor = as.actorOf(props, name = "Hello_actor")
  helloActor ! "hello"
  helloActor ! "gutentag"
  helloActor ! "buenos dias"
}