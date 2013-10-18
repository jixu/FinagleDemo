package com.yahoo.slingstone.demo

import com.twitter.finagle.{http, Service}
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.redis.util.{StringToChannelBuffer, CBToString}
import com.twitter.util.Future
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress

object PredictionServer extends App {
  implicit def s2b(str: String) = StringToChannelBuffer(str)

  val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val message = CBToString(request.getContent)
      Thread.sleep(500)
      val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      resp.setContent("Prediction(" + message + ")")
      Future.value(resp)
    }
  }

  val server = ServerBuilder()
    .codec(http.Http())
    .bindTo(new InetSocketAddress(9000))
    .name("Async Server")
    .build(service)
}
