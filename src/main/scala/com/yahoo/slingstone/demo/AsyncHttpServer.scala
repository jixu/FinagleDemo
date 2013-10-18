package com.yahoo.slingstone.demo

import com.twitter.finagle.{http, Service}
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Future
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress

object AsyncHttpServer extends App {

  implicit def s2b(str: String) = StringToChannelBuffer(str)

  val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      resp.setContent("async result")
      Future.value(resp)
    }
  }

  val server = ServerBuilder()
    .codec(http.Http())
    .bindTo(new InetSocketAddress(8080))
    .name("Async Server")
    .build(service)
}
