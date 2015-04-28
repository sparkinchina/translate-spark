/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.worker

import akka.actor.{Actor, Address, AddressFromURIString}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.SendHeartbeat
import org.apache.spark.util.ActorLogReceive

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
<<<<<<< HEAD
 * WorkerWatcher是一个连接到worker进程的Actor，如果该连接被断开，则终止当前JVM.
 * 它提供一种worker的子进程和Worker进程之间的共享生命周期的机制.
 * (运行于某个Worker节点上的Executor进程与该worker节点有相同的生命周期）
=======
>>>>>>> githubspark/branch-1.3
 */
private[spark] class WorkerWatcher(workerUrl: String)
  extends Actor with ActorLogReceive with Logging {

  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    logInfo(s"Connecting to worker $workerUrl")
    val worker = context.actorSelection(workerUrl)
    worker ! SendHeartbeat // need to send a message here to initiate connection
<<<<<<< HEAD
    // 需要在这里发送消息来初始化连接(心跳连接)
  }

  // Used to avoid shutting down JVM during tests
  // 在测试期间用来避免关闭JVM
=======
  }

  // Used to avoid shutting down JVM during tests
>>>>>>> githubspark/branch-1.3
  private[deploy] var isShutDown = false
  private[deploy] def setTesting(testing: Boolean) = isTesting = testing
  private var isTesting = false

  // Lets us filter events only from the worker's actor system
<<<<<<< HEAD
  // 让我们仅仅从这个worker的actor系统过滤事件
=======
>>>>>>> githubspark/branch-1.3
  private val expectedHostPort = AddressFromURIString(workerUrl).hostPort
  private def isWorker(address: Address) = address.hostPort == expectedHostPort

  def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receiveWithLogging = {
    case AssociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      logInfo(s"Successfully connected to $workerUrl")

    case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _)
        if isWorker(remoteAddress) =>
      // These logs may not be seen if the worker (and associated pipe) has died
<<<<<<< HEAD
      // 如果这个worker(和关联的管道)已经死掉的话，这些日志可能不可见
=======
>>>>>>> githubspark/branch-1.3
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()

    case DisassociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      // This log message will never be seen
<<<<<<< HEAD
      // 这个日志消息将永远看不到
=======
>>>>>>> githubspark/branch-1.3
      logError(s"Lost connection to worker actor $workerUrl. Exiting.")
      exitNonZero()

    case e: AssociationEvent =>
      // pass through association events relating to other remote actor systems
<<<<<<< HEAD
      // 通过关联事件连接别的远程actor系统
=======
>>>>>>> githubspark/branch-1.3

    case e => logWarning(s"Received unexpected actor system event: $e")
  }
}
