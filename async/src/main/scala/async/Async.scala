package async

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

/**
  * Acdhirr sez:
  * These riddles can be solved after reading
  * "Programming Scala 4th edition", chapter 32
  */
object Async extends AsyncInterface {

  /**
    * Transforms a successful asynchronous `Int` computation
    * into a `Boolean` indicating whether the number was even or not.
    * In case the given `Future` value failed, this method
    * should return a failed `Future` with the same error.
    */
  def transformSuccess(eventuallyX: Future[Int]): Future[Boolean] =
    eventuallyX.map(i => i%2==0)


  /**
    * Transforms a failed asynchronous `Int` computation into a
    * successful one returning `-1`.
    * Any non-fatal failure should be recovered.
    * In case the given `Future` value was successful, this method
    * should return a successful `Future` with the same value.
    */
  def recoverFailure(eventuallyX: Future[Int]): Future[Int] =
    eventuallyX.transform {
      case Success(i) => Success(i)
      case Failure(_) => Success(-1)
    }

  /**
    * Perform two asynchronous computation, one after the other. `makeAsyncComputation2`
    * should start ''after'' the `Future` returned by `makeAsyncComputation1` has
    * completed.
    * In case the first asynchronous computation failed, the second one should not even
    * be started.
    * The returned `Future` value should contain the successful result of the first and
    * second asynchronous computations, paired together.
    */
  def sequenceComputations[A, B](
    makeAsyncComputation1: () => Future[A],
    makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] = {

    for (
      // These are processed sequentially!
      // for-expressions serialize their transformations.
      // See 'Programming in Scala' 4th ed. page 703
      f1 <- makeAsyncComputation1();
      f2 <- makeAsyncComputation2()
    )
    yield (f1, f2)

    // translates to:
    // makeAsyncComputation1().flatMap(f1 => makeAsyncComputation2().map(f2=>(f1,f2)))
  }

  /**
    * Concurrently perform two asynchronous computations and pair their successful
    * result together.
    * The two computations should be started independently of each other.
    * If one of them fails, this method should return the failure.
    */
  def concurrentComputations[A, B](
    makeAsyncComputation1: () => Future[A],
    makeAsyncComputation2: () => Future[B]
  ): Future[(A, B)] = {

    // These are processed in parallel!
    val fut1 = makeAsyncComputation1()
    val fut2 = makeAsyncComputation2()

    for (
      // Now the threads have already been started above.
      f1 <- fut1;
      f2 <- fut2
    )
    yield (f1, f2)
  }

  /**
    * Attempt to perform an asynchronous computation.
    * In case of failure this method should try again to make
    * the asynchronous computation so that at most `maxAttempts`
    * are eventually performed.
    */
  def insist[A](makeAsyncComputation: () => Future[A], maxAttempts: Int): Future[A] = {

    val p: Future[A] = makeAsyncComputation.apply()

    p.transform {
      case Success(f) => Success(f)
      case Failure(_) if maxAttempts > 1 => {
        insist(makeAsyncComputation, maxAttempts-1).value.get // is a Try[A]
      }
    }
  }

  /**
    * Turns a callback-based API into a Future-based API
    * @return A `FutureBasedApi` that forwards calls to `computeIntAsync` to the `callbackBasedApi`
    *         and returns its result in a `Future` value
    *
    * Hint: Use a `Promise`
    *
    * acdhirr: See course comments:
    * https://www.coursera.org/learn/scala-akka-reactive/discussions/weeks/1/threads/rMYmuVFREeu8cg69Nt6qeQ
    *
    * acdhirr: See video
    * https://www.coursera.org/learn/scala-akka-reactive/lecture/gF5Qe/lecture-1-5-futures
    * at 12:57
    */
  def futurize(callbackBasedApi: CallbackBasedApi): FutureBasedApi = {

    val p = Promise[Int]

    new FutureBasedApi {
      override def computeIntAsync(): Future[Int] = {
        callbackBasedApi.computeIntAsync(
          i => p.complete(i)  // i is a Try[Int]
        )
        p.future
      }
    }
  }

}

/**
  * Dummy example of a callback-based API
  */
trait CallbackBasedApi {
  def computeIntAsync(continuation: Try[Int] => Unit): Unit
}

/**
  * API similar to [[CallbackBasedApi]], but based on `Future` instead
  */
trait FutureBasedApi {
  def computeIntAsync(): Future[Int]
}
