package io.cloudstate.proxy.broker

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import spray.json._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import ShoppingCartJson._

object Broker {

  final val log = LoggerFactory.getLogger(getClass)

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
  def writeToTopic(shoppingItems: immutable.Iterable[ShoppingItem])(implicit actorSystem: ActorSystem,
                                                                    materializer: Materializer) = {

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
