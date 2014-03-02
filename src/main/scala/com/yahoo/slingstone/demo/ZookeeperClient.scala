package com.yahoo.slingstone.demo

import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{Resolver, http, Service}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import com.twitter.util.Await
import com.twitter.finagle.redis.util.CBToString

class ZookeeperClient(hosts: String) {

  private val zkClient = CuratorFrameworkFactory.newClient(hosts, new ExponentialBackoffRetry(1000, 3))
  zkClient.start()

  val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
    .name("zookeeperClient")
    .codec(http.Http())
    .cluster(ZookeeperChildrenCluster(zkClient, "/finagle"))
    .hostConnectionLimit(2)
    .build()

  def apply(req: String): String = {
    val result = client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))
    CBToString(Await.result(result).getContent)
  }
}

object ZookeeperClient {
  def apply(hosts: String): ZookeeperClient = {
    new ZookeeperClient(hosts)
  }
}
