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

package com.lightbend.statefulserverless

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, CoordinatedShutdown, Props, Status, Terminated}
import akka.cluster.Cluster
import akka.util.Timeout
import akka.pattern.{pipe, ask}
import akka.stream.Materializer
import akka.http.scaladsl.{Http, HttpConnectionContext, UseHttp2}
import akka.http.scaladsl.Http.ServerBinding
import akka.cluster.sharding._
import akka.grpc.GrpcClientSettings
import com.typesafe.config.Config
import com.lightbend.statefulserverless.grpc._
import com.google.protobuf.empty.Empty
import com.lightbend.statefulserverless.ConcurrencyEnforcer.ConcurrencyEnforcerSettings
import com.lightbend.statefulserverless.StatsCollector.StatsCollectorSettings

import scala.concurrent.duration._

object ServerManager {
  final case class Configuration (
    httpInterface: String,
    httpPort: Int,
    userFunctionInterface: String,
    userFunctionPort: Int,
    relayTimeout: Timeout,
    relayOutputBufferSize: Int,
    gracefulTerminationTimeout: Timeout,
    passivationTimeout: Timeout,
    numberOfShards: Int,
    proxyParallelism: Int,
    concurrencySettings: ConcurrencyEnforcerSettings,
    statsCollectorSettings: StatsCollectorSettings
  ) {
    validate()
    def this(config: Config) = {
      this(
        httpInterface              = config.getString("http-interface"),
        httpPort                   = config.getInt("http-port"),
        userFunctionInterface      = config.getString("user-function-interface"),
        userFunctionPort           = config.getInt("user-function-port"),
        relayTimeout               = Timeout(config.getDuration("relay-timeout").toMillis.millis),
        relayOutputBufferSize      = config.getInt("relay-buffer-size"),
        gracefulTerminationTimeout = Timeout(config.getDuration("graceful-termination-timeout").toMillis.millis),
        passivationTimeout         = Timeout(config.getDuration("passivation-timeout").toMillis.millis),
        numberOfShards             = config.getInt("number-of-shards"),
        proxyParallelism           = config.getInt("proxy-parallelism"),
        concurrencySettings        = ConcurrencyEnforcerSettings(
          concurrency   = config.getInt("container-concurrency"),
          actionTimeout = config.getDuration("action-timeout").toMillis.millis,
          cleanupPeriod = config.getDuration("action-timeout-poll-period").toMillis.millis
        ),
        statsCollectorSettings     = new StatsCollectorSettings(config.getConfig("stats"))
      )
    }

    private[this] final def validate(): Unit = {
      require(proxyParallelism > 0, s"proxy-parallelism must be greater than 0 but was $proxyParallelism")
      require(numberOfShards > 0, s"number-of-shards must be greater than 0 but was $numberOfShards")
      require(relayOutputBufferSize > 0, "relay-buffer-size must be greater than 0 but was $relayOutputBufferSize")
    }
  }

  def props(config: Configuration)(implicit mat: Materializer): Props = Props(new ServerManager(config))

  final case object Ready // Responds with true / false
}

class ServerManager(config: ServerManager.Configuration)(implicit mat: Materializer) extends Actor with ActorLogging {
  import context.system
  import context.dispatcher
  import ServerManager.Ready

  private[this] final val clientSettings = GrpcClientSettings.connectToServiceAt(config.userFunctionInterface, config.userFunctionPort).withTls(false)
  private[this] final val client         = EntityClient(clientSettings)
  private[this] final val statsCollector = context.actorOf(StatsCollector.props(config.statsCollectorSettings), "stats-collector")
  private[this] final val concurrencyEnforcer = context.actorOf(ConcurrencyEnforcer.props(config.concurrencySettings, statsCollector), "concurrency-enforcer")

  client.ready(Empty.of()) pipeTo self

  override def receive: Receive = {
    case reply: EntitySpec =>
      log.debug("Received EntitySpec from user function")
      val stateManagerConfig = StateManager.Configuration(reply.persistenceId, config.passivationTimeout, config.relayOutputBufferSize)

      log.debug("Starting StateManager for {}", reply.persistenceId)
      val clusterSharding = ClusterSharding(system)
      val clusterShardingSettings = ClusterShardingSettings(system)
      val stateManager = context.watch(clusterSharding.start(
        typeName = reply.persistenceId,
        entityProps = StateManagerSupervisor.props(client, stateManagerConfig, concurrencyEnforcer, statsCollector),
        settings = clusterShardingSettings,
        messageExtractor = new Serve.RequestMessageExtractor(config.numberOfShards),
        allocationStrategy = new DynamicLeastShardAllocationStrategy(1, 10, 3, 0.0),
        handOffStopMessage = StateManager.Stop
      ))

      log.debug("Creating gRPC proxy for {}", reply.persistenceId)

      val handler = Serve.createRoute(stateManager, statsCollector, config.proxyParallelism, config.relayTimeout, reply)

      log.debug("Starting gRPC proxy for {}", reply.persistenceId)

      // Don't actually bind until we have a cluster
      Cluster(context.system).registerOnMemberUp {
        Http().bindAndHandleAsync(
          handler = handler,
          interface = config.httpInterface,
          port = config.httpPort,
          connectionContext = HttpConnectionContext(http2 = UseHttp2.Always)
        ) pipeTo self
      }

      context.become(binding(stateManager))
    case Ready => sender ! false
    case Status.Failure(cause) =>
      // Failure to load the entity spec is not fatal, simply crash and let the backoff supervisor restart us
      throw cause
  }

  private[this] final def binding(stateManager: ActorRef): Receive = {
    case binding: ServerBinding =>
      log.info(s"StatefulServerless backend online at ${binding.localAddress}")

      // These can be removed if https://github.com/akka/akka-http/issues/1210 ever gets implemented
      val shutdown = CoordinatedShutdown(system)

      shutdown.addTask(CoordinatedShutdown.PhaseServiceUnbind, "http-unbind") { () =>
        binding.unbind().map(_ => Done)
      }

      shutdown.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "http-graceful-terminate") { () =>
        binding.terminate(config.gracefulTerminationTimeout.duration).map(_ => Done)
      }

      shutdown.addTask(CoordinatedShutdown.PhaseServiceStop, "http-shutdown") { () =>
        Http().shutdownAllConnectionPools().map(_ => Done)
      }

      context.become(running(stateManager))

    case Terminated(`stateManager`) =>
      log.error("StateManager terminated during initialization of server")
      system.terminate()

    case Status.Failure(cause) =>
      // Failure to bind the HTTP server is fatal, terminate
      log.error(cause, "Failed to bind HTTP server")
      system.terminate()

    case Ready => sender ! false
  }

  /** Nothing to do when running */
  private[this] final def running(stateManager: ActorRef): Receive = {
    case Terminated(`stateManager`) => // TODO How to handle the termination of the stateManager during runtime?
    case Ready => sender ! true
  }

  override final def postStop(): Unit = {
    super.postStop()
    client.close()
    log.debug("shutting down")
    system.terminate()
  }
}
