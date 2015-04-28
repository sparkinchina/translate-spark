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

<<<<<<< HEAD
import akka.actor._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 */
/**
 * 为了启动driver程序的工具对象诸如它们和Worker进程共享状态 .
=======
import java.io.File

import akka.actor._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{AkkaUtils, ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 * This is used in standalone cluster mode only.
>>>>>>> githubspark/branch-1.3
 */
object DriverWrapper {
  def main(args: Array[String]) {
    args.toList match {
<<<<<<< HEAD
      case workerUrl :: mainClass :: extraArgs =>
=======
      /*
       * IMPORTANT: Spark 1.3 provides a stable application submission gateway that is both
       * backward and forward compatible across future Spark versions. Because this gateway
       * uses this class to launch the driver, the ordering and semantics of the arguments
       * here must also remain consistent across versions.
       */
      case workerUrl :: userJar :: mainClass :: extraArgs =>
>>>>>>> githubspark/branch-1.3
        val conf = new SparkConf()
        val (actorSystem, _) = AkkaUtils.createActorSystem("Driver",
          Utils.localHostName(), 0, conf, new SecurityManager(conf))
        actorSystem.actorOf(Props(classOf[WorkerWatcher], workerUrl), name = "workerWatcher")

<<<<<<< HEAD
        // Delegate to supplied main class
        // 委托调用到提供的主类
        val clazz = Class.forName(args(1))
=======
        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI().toURL()
        val loader =
          if (sys.props.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
            new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
          } else {
            new MutableURLClassLoader(Array(userJarUrl), currentLoader)
          }
        Thread.currentThread.setContextClassLoader(loader)

        // Delegate to supplied main class
        val clazz = Class.forName(mainClass, true, loader)
>>>>>>> githubspark/branch-1.3
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])

        actorSystem.shutdown()

      case _ =>
<<<<<<< HEAD
        System.err.println("Usage: DriverWrapper <workerUrl> <driverMainClass> [options]")
=======
        System.err.println("Usage: DriverWrapper <workerUrl> <userJar> <driverMainClass> [options]")
>>>>>>> githubspark/branch-1.3
        System.exit(-1)
    }
  }
}
