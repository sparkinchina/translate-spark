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

import java.io._

import scala.collection.JavaConversions._

import akka.actor.ActorRef
import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.deploy.{ApplicationDescription, Command, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 * 管理这个executor进程的执行.
 * 这个类当前仅仅用在standalone模式.
 */
private[spark] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: ActorRef,
    val workerId: String,
    val host: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    val conf: SparkConf,
    var state: ExecutorState.Value)
  extends Logging {

  val fullId = appId + "/" + execId
  var workerThread: Thread = null
  var process: Process = null
  var stdoutAppender: FileAppender = null
  var stderrAppender: FileAppender = null

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  // NOTE: 由Executor程序强制执行自动关闭现在是冗余的。将来删除这个看起来才更合理.
  var shutdownHook: Thread = null

  def start() {
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    // 关闭回调在关闭时杀掉actors.
    shutdownHook = new Thread() {
      override def run() {
        killProcess(Some("Worker shutting down"))
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdownHook)
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   *
   * @param message the exception message which caused the executor's death 
   */

  /**
   * 杀掉executor进程，等待离开并通知worker来更新资源状态.
   *
   * @param message 这个是引起executor的死掉的异常消息
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      process.destroy()
      process.waitFor()
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Some(process.waitFor())
    }
    worker ! ExecutorStateChanged(appId, execId, state, message, exitCode)
  }

  /** Stop this executor runner, including killing the process it launched */
  /** 关掉这个executor运行器，包括在这个进程启动时杀掉它 */
  def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      // 当中断时workerThread将杀死子进程
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  /** 一个命令参数内替换变量如{ { EXECUTOR_ID } }和{ {CORES} }后传递给我们 */
  def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   */
  /**
   * 下载并运行我们ApplicationDescription描述的executor
   * 实际就是创建ExecutorBackend
   */
  def fetchAndRunExecutor() {
    try {
      // Launch the process
      // 启动这个进程
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, memory,
        sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\""))

      builder.directory(executorDir)
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      // 在Spark Shell中运行这个类时, 避免为executor命令创建一个"scala"父进程
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

      // Redirect its stdout and stderr to files
      // 重定向它的stdout和stderr到文件中
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      // 等待他离开; executor 可能会带着0编码退出(当driver指示它关闭时)或者带着非零编码退出
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      worker ! ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }
}
