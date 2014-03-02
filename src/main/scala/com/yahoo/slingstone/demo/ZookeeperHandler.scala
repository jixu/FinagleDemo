package com.yahoo.slingstone.demo

import org.jboss.netty.channel.{ChannelFutureListener, MessageEvent, ChannelHandlerContext, SimpleChannelHandler}
import com.twitter.finagle.redis.util.StringToChannelBuffer

class ZookeeperHandler extends SimpleChannelHandler {
  import ZookeeperHandler._
  implicit def s2b(str: String) = StringToChannelBuffer(str)
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val result = zookeeperClient("get host name")
    println(result)

    val ch = ctx.getChannel
    val chFuture = ch.write(s2b(result))
    chFuture.addListener(ChannelFutureListener.CLOSE)
  }
}

object ZookeeperHandler {
  val zookeeperClient = ZookeeperClient("127.0.0.1:2181")
}