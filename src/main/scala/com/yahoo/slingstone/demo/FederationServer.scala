package com.yahoo.slingstone.demo

import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

import com.twitter.finagle._
import com.twitter.util.{Throw, Duration, Await, Future}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis.util.{CBToString, ReplyFormat, StringToChannelBuffer}
import com.twitter.finagle.redis.protocol._

import scala.util.Random

object FederationServer extends App {
  lazy val relevenceClient: Service[HttpRequest, HttpResponse] =
    Http.newService("127.0.0.1:8000")
  lazy val popularityClient = ClientBuilder()
    .name("redis-client")
    .codec(redis.Redis())
    .hosts("127.0.0.1:6379")
    .hostConnectionLimit(2)
    .retries(2)
    .build()
  lazy val predictionClient: Service[HttpRequest, HttpResponse] = ClientBuilder()
    .name("prediction-client")
    .codec(http.Http())
    .hosts("127.0.0.1:8081")
    .hostConnectionLimit(2)
    .timeout(Duration.fromMilliseconds(500))
    .build()

  implicit def s2b(s: String) = StringToChannelBuffer(s)
  
  def fetchUuidsFromPopularityServer(): Future[Seq[String]] = popularityClient(LRange("poptops", 0, -1)) flatMap {
    _ match {
      case MBulkReply(msgs) =>
        Future.value(ReplyFormat.toString(msgs))
      case _ => Future.exception(new RuntimeException("Not MBulkReply"))
    }
  } rescue {
    case _: FailedFastException =>
      println("Fetch Uuids from Popularity Server failed. Use Empty result for this call.")
      Future.value(Seq[String]())
  }

  def fetchUuidsFromRelevanceServer(): Future[Seq[String]] = {
    val relevanceReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uuids_from_relevance_server")
    relevenceClient(relevanceReq) flatMap { response =>
      Future.value(CBToString(response.getContent).stripLineEnd.split(",").toSeq)
    } rescue {
      case _: FailedFastException =>
        println("Fetch Uuids from Relevance Server failed. Use Empty result for this call.")
        Future.value(Seq[String]())
      case _: ChannelWriteException =>
        println("Fetch Uuids from Relevance Server failed. Use Empty result for this call.")
        Future.value(Seq[String]())
    }
  }

  def mergeResults(resultSeq: List[Future[Seq[String]]]): Future[Seq[String]] =
    Future.collect(resultSeq) flatMap { seq =>
      Future.value(seq.flatten.toSet.toSeq.sorted)
    }

  def fetchPredictionResult(uuids: Seq[String]): Future[Seq[String]] = {
    val predictionReq = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    val cb: ChannelBuffer = ChannelBuffers.copiedBuffer(uuids.mkString(","))
    predictionReq.setHeader(HttpHeaders.Names.CONTENT_LENGTH, cb.readableBytes())
    predictionReq.setContent(cb)

    predictionClient(predictionReq) flatMap { resp =>
      Future.value(CBToString(resp.getContent).stripLineEnd.split(",").toSeq)
    } rescue {
      case _: FailedFastException =>
        println("Fetch Result from Prediction Server failed. Use Default Ranking.")
        Future.value(uuids)
      case _: ChannelWriteException =>
        println("Fetch Result from Prediction Server failed. Use Default Ranking.")
        Future.value(uuids)
      case e: GlobalRequestTimeoutException =>
        println("Fetch Result from Prediction Server timeout. Use Default Ranking.")
        Future.value(uuids)
    }
  }

  val federationService: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = mergeResults(List(fetchUuidsFromPopularityServer(), fetchUuidsFromRelevanceServer())) flatMap { seq =>
      fetchPredictionResult(seq) flatMap { newSeq =>
        val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        resp.setContent(newSeq.mkString(",") + "\n")
        Future.value(resp)
      }
    }
  }

  val mockPredictionService: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest) = {
      val uuids = CBToString(request.getContent).stripLineEnd.split(",").toSeq
      Thread.sleep(Random.nextInt(125 * uuids.length))
      val resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      resp.setContent(Random.shuffle(uuids).mkString(",") + "\n")
      Future.value(resp)
    }
  }

  val federationServer = ServerBuilder()
    .codec(http.Http())
    .bindTo(new InetSocketAddress(8080))
    .name("FederationServer")
    .build(federationService)

  val mockPredictionServer = ServerBuilder()
    .codec(http.Http())
    .bindTo(new InetSocketAddress(8081))
    .name("MockPredictionServer")
    .build(mockPredictionService)
}