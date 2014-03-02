package com.yahoo.slingstone.demo

import org.apache.curator.framework.CuratorFramework
import com.twitter.finagle.builder.Cluster
import java.net.{InetSocketAddress, SocketAddress}
import com.twitter.logging.Logger
import scala.collection.{Seq, mutable}
import com.twitter.util._
import com.twitter.concurrent.Spool
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import org.apache.curator.utils.ZKPaths

/**
 * A finagle cluster uses all zookeeper children under a specified node as service
 * An example:
 *   > ls /finagle
 *   [host1:4080,host2:4080]
 *   host1:4080 and host2:4080 are both in this cluster.
 * @author xuji
 */
class ZookeeperChildrenCluster(zkClient: CuratorFramework, path: String) extends Cluster[SocketAddress] {
  val log = Logger(this.getClass)

  private val underlyingSet = new mutable.HashSet[SocketAddress]
  private var changes = new Promise[Spool[Cluster.Change[SocketAddress]]]

  private val pathCache = new PathChildrenCache(zkClient, path, false)
  pathCache.getListenable.addListener(new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
      /**
       * method to create a InetSocketAddress using a znode
       */
      def generateAddress(hostAndPort: String): Try[InetSocketAddress] = {
        Try {
          val pos = hostAndPort.indexOf(':')
          val host = hostAndPort.substring(0, pos)
          val port = hostAndPort.substring(pos+1).toInt
          new InetSocketAddress(host, port)
        }
      }

      import PathChildrenCacheEvent.Type._
      event.getType match {
        case CHILD_ADDED =>
          val path = event.getData.getPath
          val hostAndPort = ZKPaths.getNodeFromPath(path)
          log.info("host added in zookeeper: " + path)
          generateAddress(hostAndPort) match {
            case Return(address) =>
              addAddress(address)
            case Throw(e) =>
              log.error("An error happened when generating address: " + e.getStackTraceString)
          }
        case CHILD_REMOVED =>
          val path = event.getData.getPath
          val hostAndPort = ZKPaths.getNodeFromPath(path)
          log.info("host removed from zookeeper: " + path)
          generateAddress(hostAndPort) match {
            case Return(address) =>
              removeAddress(address)
            case Throw(e) =>
              log.error("An error happened when generating address: " + e.getStackTraceString)
          }
        case _ =>
          log.warning("Unexpected event:" + event.getData)
      }
    }
  })
  pathCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)

  private def appendUpdate(update: Cluster.Change[SocketAddress]) = {
    val newTail = new Promise[Spool[Cluster.Change[SocketAddress]]]
    changes() = Return(update *:: newTail)
    changes = newTail
  }

  private def addAddress(address: SocketAddress) = synchronized {
    underlyingSet += address
    appendUpdate(Cluster.Add(address))
  }

  private def removeAddress(address: SocketAddress) = synchronized {
    underlyingSet -= address
    appendUpdate(Cluster.Rem(address))
  }

  def snap: (Seq[SocketAddress], Future[Spool[Cluster.Change[SocketAddress]]]) =
    synchronized {
      (underlyingSet.toSeq, changes)
    }
}

object ZookeeperChildrenCluster {
  def apply(zkClient: CuratorFramework, path: String) = new ZookeeperChildrenCluster(zkClient, path)
}
