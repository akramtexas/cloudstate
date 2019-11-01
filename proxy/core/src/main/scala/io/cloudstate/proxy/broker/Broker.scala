package io.cloudstate.proxy.broker

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import spray.json._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import ShoppingCartJson._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, MediaRanges}
import akka.util.ByteString
import org._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.testcontainers.containers.KafkaContainer

import scala.util.control.NonFatal

object Broker extends App with DefaultJsonProtocol {

  val log = LoggerFactory.getLogger(getClass)

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  // To get things started (and before wiring things up further), let's create some records for this PoC.
  val shoppingItems =
    List(ShoppingItem("joe", "can-opener"), ShoppingItem("akram", "hairbrush"), ShoppingItem("aardvark", "nuts"))

  // TODO: An in-memory version (for ease-of-testing), based on a common superset, interface-wise.
  println("Using Alpakka Kafka client for the default broker implementation.")

  println("The client is writing (to an output Kafka topic) same shopping item-related records.")
  val writing: Future[Done] = writeToTopic(shoppingItems)
  Await.result(writing, 10.seconds)

  println("The client is reading (from an input Kafka topic) those shopping item-related records.")
  readFromTopic()

//  implicit val actorSystem = ActorSystem("alpakka-samples")

  import actorSystem.dispatcher

  implicit val mat: Materializer = ActorMaterializer()

  // TODO: Replace the following bootstrap code, which is taken from: https://github.com/akka/alpakka-samples
  val httpRequest =
    HttpRequest(uri = "https://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download")
      .withHeaders(Accept(MediaRanges.`text/*`))

  def extractEntityData(response: HttpResponse): Source[ByteString, _] =
    response match {
      case HttpResponse(OK, _, entity, _) => entity.dataBytes
      case notOkResponse =>
        Source.failed(new RuntimeException(s"illegal response $notOkResponse"))
    }

  def cleanseCsvData(csvData: Map[String, ByteString]): Map[String, String] =
    csvData
      .filterNot { case (key, _) => key.isEmpty }
      .mapValues(_.utf8String)

  def toJson(map: Map[String, String])(implicit jsWriter: JsonWriter[Map[String, String]]): JsValue =
    jsWriter.write(map)

  val kafkaBroker: KafkaContainer = new KafkaContainer()
  kafkaBroker.start()

  private val bootstrapServers: String = kafkaBroker.getBootstrapServers()

  val kafkaProducerSettings = ProducerSettings(actorSystem, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  //  val future: Future[Done] =
  //    Source
  //      .single(httpRequest) //: HttpRequest
  //      .mapAsync(1)(Http().singleRequest(_)) //: HttpResponse
  //      .flatMapConcat(extractEntityData) //: ByteString
  //      .via(CsvParsing.lineScanner()) //: List[ByteString]
  //      .via(CsvToMap.toMap()) //: Map[String, ByteString]
  //      .map(cleanseCsvData) //: Map[String, String]
  //      .map(toJson) //: JsValue
  //      .map(_.compactPrint) //: String (JSON formatted)
  //      .map { elem =>
  //        new ProducerRecord[String, String]("topic1", elem) //: Kafka ProducerRecord
  //      }
  //      .runWith(Producer.plainSink(kafkaProducerSettings))

  val cs: CoordinatedShutdown = CoordinatedShutdown(actorSystem)
  cs.addTask(CoordinatedShutdown.PhaseServiceStop, "shut-down-client-http-pool")(
    () => Http().shutdownAllConnectionPools().map(_ => Done)
  )

  val kafkaConsumerSettings = ConsumerSettings(actorSystem, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("topic1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val control = Consumer
    .atMostOnceSource(kafkaConsumerSettings, Subscriptions.topics("topic1"))
    .map(_.value)
    .toMat(Sink.foreach(println))(Keep.both)
    .mapMaterializedValue(Consumer.DrainingControl.apply)
    .run()

  for {
//    _ <- future
    _ <- control.drainAndShutdown()
  } {
    kafkaBroker.stop()
    cs.run(CoordinatedShutdown.UnknownReason)
  }

  // TODO: Introduce a mechanism for enlightened looping, complete with exit strategy and failure-handling.

  /**
   *
   * Write records to output_topic.
   *
   * @param shoppingItems
   * @param actorSystem
   * @param materializer
   * @return
   */
  def writeToTopic(
      shoppingItems: immutable.Iterable[ShoppingItem]
  )(implicit actorSystem: ActorSystem, materializer: Materializer) = {

    val log = LoggerFactory.getLogger(getClass)

    // Kafka option of output_topic: "gotten_carts" was defined in shoppingcart.proto
    val bootstrapServers = "localhost:9094"
    val topic = "gotten_carts"
    implicit val system: ActorSystem = ActorSystem("CloudStateTCK")

    // Kafka producer configurations
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

    val producing: Future[Done] = Source(shoppingItems)
      .map { shoppingItem =>
        log.debug("producing {}", shoppingItem)
        new ProducerRecord(topic, shoppingItem.productId, shoppingItem.userID)
      }
      .runWith(Producer.plainSink(producerSettings))

    producing.foreach(_ => log.info("Producing finished"))(actorSystem.dispatcher)

    producing
  }

  /**
   *
   * Read records from input_topic.
   *
   * @return
   */
  def readFromTopic(): Source[ShoppingItem, Consumer.Control] = {

    implicit val system: ActorSystem = ActorSystem("CloudStateTCK")

    // Kafka option of input_topic: "get_carts" was defined in shoppingcart.proto
    val bootstrapServers = "localhost:9094"
    val topic = "get_carts"

    // Kafka consumer configurations
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)

    val resume = ActorAttributes.withSupervisionStrategy {
      case _: spray.json.JsonParser.ParsingException => Supervision.Resume
      case _ => Supervision.stop
    }

    val consumer: Source[ShoppingItem, Consumer.Control] = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(topic))
      .map { record =>
        val value = record.value()
        val sampleData = value.parseJson.convertTo[ShoppingItem]
        sampleData
      }
      .withAttributes(resume)

    consumer
  }
}
