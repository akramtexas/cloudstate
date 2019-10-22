/*
 * Copyright 2019 Lightbend Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cloudstate.proxy.broker

import akka.actor.{ActorRef, ActorSystem}

import scala.concurrent.duration._
import akka.{ConfigurationException, Done, NotUsed}
import akka.util.Timeout
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Flow
import org.scalatest._
import akka.testkit.TestProbe
import io.cloudstate.proxy.test._
import com.google.protobuf.Descriptors.{FileDescriptor, ServiceDescriptor}
import com.google.protobuf.empty.Empty
import io.cloudstate.protocol.entity.{EntityDiscovery, EntityDiscoveryClient, EntitySpec, ProxyInfo, UserFunctionError}
import io.cloudstate.proxy.EntityDiscoveryManager.ServableEntity
import io.cloudstate.proxy.entity.{UserFunctionCommand, UserFunctionReply}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.MustMatchers
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethods,
  HttpProtocols,
  HttpRequest,
  HttpResponse,
  StatusCodes
}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import kafka.server.ConfigType

import scala.concurrent.{ExecutionContext, Future}

class BrokerSpec extends BaseSpec with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("CloudStateTCK")
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  def validateResponse(response: HttpResponse): Future[String] =
    Unmarshal(response).to[String]
//    println("GET a request" + Unmarshal(response).to[String])

  /**
   *
   *  - GET a request
   *  - WRITE to input_topic
   *  - TRANSFORM into HttpRequest
   *  - HAVE it processed by command
   *  - TRANSFORM into HttpResponse
   *  - WRITE to output_topic
   *
   */
  println("GET a request. Simply sending a constructed HttpRequest in.")
  Http()
    .singleRequest(
      HttpRequest(
        method = HttpMethods.GET,
        headers = Nil,
        uri = "uri",
        entity = HttpEntity.Empty,
        protocol = HttpProtocols.`HTTP/1.1`
      )
    )
    .flatMap(validateResponse)

  val shoppingItems =
    List(ShoppingItem("joe", "can-opener"), ShoppingItem("akram", "hairbrush"), ShoppingItem("aardvark", "nuts"))

//  BrokerUtils.

  println("WRITE to input_topic")
  Broker.writeToTopic(shoppingItems)

  println("READ from input_topic")
  Broker.readFromTopic()

  println("HAVE it be processed by command")

  println("TRANSFORM into HttpResponse")

  println("Get the HttpResponse")

  println("WRITE to output_topic")
  Broker.writeToTopic(shoppingItems)
}
