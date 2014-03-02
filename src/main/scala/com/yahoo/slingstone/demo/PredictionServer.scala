package com.yahoo.slingstone.demo

import com.twitter.finagle.{http, Service}
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.redis.util.{StringToChannelBuffer, CBToString}
import com.twitter.util.{Await, Future}
import com.twitter.finagle.builder.ServerBuilder
import java.net.InetSocketAddress

import com.twitter.common.zookeeper.{ServerSetImpl, ZooKeeperClient}
import com.twitter.common.quantity.{Amount,Time}

object PredictionServer extends App {
  implicit def s2b(str: String) = StringToChannelBuffer(str)
  val port = args(0)

  val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val message = CBToString(request.getContent)
      val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      resp.setContent("port is " + port)
      Future.value(resp)
    }
  }

  val address = new InetSocketAddress(port.toInt)
  val server = ServerBuilder()
    .codec(http.Http())
    .bindTo(address)
    .name("Prediction server" + port)
    .build(service)
}
