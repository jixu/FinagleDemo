package com.yahoo.slingstone.demo

import org.jboss.netty.channel._
import com.twitter.util.{Await, Future, Duration, FuturePool}
import com.yahoo.slingstone.demo.FederationHandler._
import com.twitter.finagle._
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.util.{StringToChannelBuffer, CBToString}
import com.twitter.finagle.ChannelWriteException

class FederationHandler extends SimpleChannelHandler {
  implicit def s2b(str: String) = StringToChannelBuffer(str)
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    println("Got a message: " + e.getMessage)

    val expensiveResult: Future[String] = futurePool {
      expensiveComputation(500)
    }
    val asyncResult: Future[String] = doAsyncCall()
    val resultFuture: Future[String] = Future.join(expensiveResult, asyncResult) map { strPair =>
      strPair._1 + " : " + strPair._2 }

    val result = Await.result(resultFuture)
    println(result)

    val ch = ctx.getChannel
    val chFuture = ch.write(s2b(result))
    chFuture.addListener(ChannelFutureListener.CLOSE)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = {
    e.getCause.printStackTrace()
    e.getChannel.close()
  }

  def doAsyncCall(): Future[String] = {
    val asyncReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    asyncClient(asyncReq) flatMap { response =>
      Future.value(CBToString(response.getContent))
    } rescue {
      case _: FailedFastException | _: ChannelWriteException | _: GlobalRequestTimeoutException =>
        println("Fetch async result error")
        Future.value("Error in async call")

    }
  }

  def expensiveComputation(timeInterval: Long): String = {
    val timeStart = System.currentTimeMillis
    var str = "Expensive Computation"
    while (System.currentTimeMillis() - timeStart < timeInterval) {
      // println(System.currentTimeMillis())
      1 until 10000 foreach {i => str = str.reverse}
    }
    str
  }
}

object FederationHandler {
  val futurePool = FuturePool.unboundedPool
  val asyncClient: Service[HttpRequest, HttpResponse] = ClientBuilder()
    .name("async-client")
    .codec(http.Http())
    .hosts("127.0.0.1:8080")
    .hostConnectionLimit(100)
    .timeout(Duration.fromMilliseconds(1))
    .build()
}
