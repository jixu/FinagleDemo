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
      resp.setContent("{\"result\":\"ok\",\"resultCode\":0,\"popularDocs\":[{\"uuid\":\"ccc829cd-bb98-345b-ad7e-64ea86978961\",\"score\":0.7549561452977871,\"title\":\"ccc829cd-bb98-345b-ad7e-64ea86978961\",\"reason\":\"age_gender\",\"features\":[]}]}")
      Future.value(resp)
    }
  }

  val server = ServerBuilder()
    .codec(http.Http())
    .bindTo(new InetSocketAddress(80))
    .name("Async Server")
    .build(service)
}
