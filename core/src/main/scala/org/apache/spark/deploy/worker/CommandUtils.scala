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

import java.io.{File, FileOutputStream, InputStream, IOException}
import java.lang.System._

import scala.collection.Map

import org.apache.spark.Logging
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils

/**
 ** Utilities for running commands with the spark classpath.
 *
 * 运行spark的classpath下命令的工具类.
 */
private[spark]
object CommandUtils extends Logging {

  /**
   * Build a ProcessBuilder based on the given parameters.
   * The `env` argument is exposed for testing.
   *
   * 基于给定的参数构建一个ProcessBuilder对象.
   * `env` 参数是为了测试目的而暴露的.
   */
  def buildProcessBuilder(
      command: Command,
      memory: Int,
      sparkHome: String,
      substituteArguments: String => String,
      classPaths: Seq[String] = Seq[String](),
      env: Map[String, String] = sys.env): ProcessBuilder = {
    val localCommand = buildLocalCommand(command, substituteArguments, classPaths, env)
    val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)
    val builder = new ProcessBuilder(commandSeq: _*)
    val environment = builder.environment()
    for ((key, value) <- localCommand.environment) {
      environment.put(key, value)
    }
    builder
  }

  private def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    val runner = sys.env.get("JAVA_HOME").map(_ + "/bin/java").getOrElse("java")

    // SPARK-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    // SPARK-698: 不要调用这个run.cmd脚本,
    // 在windows系统上process.destroy()未能杀死进程树
    Seq(runner) ++ buildJavaOpts(command, memory, sparkHome) ++ Seq(command.mainClass) ++
      command.arguments
  }

  /**
   * Build a command based on the given one, taking into account the local environment
   * of where this command is expected to run, substitute any placeholders, and append
   * any extra class paths.
   *
   * 构建一个基于给定的一个命令的command对象,考虑本地环境,这个命令预期会运行,同时
   * 替代任何占位符并且附加任何额外的类路径。
   */
  private def buildLocalCommand(
      command: Command,
      substituteArguments: String => String,
      classPath: Seq[String] = Seq[String](),
      env: Map[String, String]): Command = {
    val libraryPathName = Utils.libraryPathEnvName
    val libraryPathEntries = command.libraryPathEntries
    val cmdLibraryPath = command.environment.get(libraryPathName)

    val newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment + ((libraryPathName, libraryPaths.mkString(File.pathSeparator)))
    } else {
      command.environment
    }

    Command(
      command.mainClass,
      command.arguments.map(substituteArguments),
      newEnvironment,
      command.classPathEntries ++ classPath,
      Seq[String](), // library path already captured in environment variable
      command.javaOpts)
  }

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   *
   * 注意: 这个方法总是匹配这个在运行脚本中的环境变量并且采用拼装JAVA_OPTS变量的方式.
   */
  private def buildJavaOpts(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    val memoryOpts = Seq(s"-Xms${memory}M", s"-Xmx${memory}M")

    // Exists for backwards compatibility with older Spark versions
    // 存在为了对老的Spark版本的向后兼容性
    val workerLocalOpts = Option(getenv("SPARK_JAVA_OPTS")).map(Utils.splitCommandString)
      .getOrElse(Nil)
    if (workerLocalOpts.length > 0) {
      logWarning("SPARK_JAVA_OPTS was set on the worker. It is deprecated in Spark 1.0.")
      logWarning("Set SPARK_LOCAL_DIRS for node-specific storage locations.")
    }

    // Figure out our classpath with the external compute-classpath script
    // 找出我们与外部计算出的classpath脚本的类路径中
    val ext = if (System.getProperty("os.name").startsWith("Windows")) ".cmd" else ".sh"
    val classPath = Utils.executeAndGetOutput(
      Seq(sparkHome + "/bin/compute-classpath" + ext),
      extraEnvironment = command.environment)
    val userClassPath = command.classPathEntries ++ Seq(classPath)

    val javaVersion = System.getProperty("java.version")
    val permGenOpt = if (!javaVersion.startsWith("1.8")) Some("-XX:MaxPermSize=128m") else None
    Seq("-cp", userClassPath.filterNot(_.isEmpty).mkString(File.pathSeparator)) ++
      permGenOpt ++ workerLocalOpts ++ command.javaOpts ++ memoryOpts
  }

  /** Spawn a thread that will redirect a given stream to a file */
  /** 产生一个线程将给定流重定向到一个文件 */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file, true)
    // TODO: It would be nice to add a shutdown hook here that explains why the output is
    //       terminating. Otherwise if the worker dies the executor logs will silently stop.
    // TODO: 在这儿添加一个解释为什么这个输出终结的shutdown回调将会比较好一些，否则如果这个worker
    //        死掉这个executor日志将会静默关闭.
    new Thread("redirect output to " + file) {
      override def run() {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }
}
