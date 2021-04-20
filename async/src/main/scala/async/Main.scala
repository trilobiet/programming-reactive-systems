package async

object Main extends App {
  println(Async.transformSuccess(concurrent.Future.successful(3)))
}