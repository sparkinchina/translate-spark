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

package org.apache.spark.util.logging

import java.io.{File, FileFilter, InputStream}

import com.google.common.io.Files
import org.apache.spark.SparkConf
import RollingFileAppender._

/**
 * Continuously appends data from input stream into the given file, and rolls
 * over the file after the given interval. The rolled over files are named
 * based on the given pattern.
 *
 * @param inputStream             Input stream to read data from
 * @param activeFile              File to write data to
 * @param rollingPolicy           Policy based on which files will be rolled over.
 * @param conf                    SparkConf that is used to pass on extra configurations
 * @param bufferSize              Optional buffer size. Used mainly for testing.
 */
/**
 * 连续不断地从输入流向给定的文件追加数据, 然后在指定的时间间隔后翻转这个文件.翻转文件是
 * 基于给定的模式命名的.
 *
 * @param inputStream               读取数据的输入流
 * @param activeFile                写数据的目标文件
 * @param rollingPolicy             基于哪些将被翻转文件的策略.
 * @param conf                        SparkConf 用来传入额外的配置项
 * @param bufferSize                可选的缓冲大小. 主要用于测试..
 */
private[spark] class RollingFileAppender(
    inputStream: InputStream,
    activeFile: File,
    val rollingPolicy: RollingPolicy,
    conf: SparkConf,
    bufferSize: Int = DEFAULT_BUFFER_SIZE
  ) extends FileAppender(inputStream, activeFile, bufferSize) {

  private val maxRetainedFiles = conf.getInt(RETAINED_FILES_PROPERTY, -1)

  /** Stop the appender */
  /** 关闭这个appender */
  override def stop() {
    super.stop()
  }

  /** Append bytes to file after rolling over is necessary */
  /** 在翻转是必要的之后追加字节 */
  override protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (rollingPolicy.shouldRollover(len)) {
      rollover()
      rollingPolicy.rolledOver()
    }
    super.appendToFile(bytes, len)
    rollingPolicy.bytesWritten(len)
  }

  /** Rollover the file, by closing the output stream and moving it over */
  /** 靠关闭输出流和挪动它来翻转这个文件 */
  private def rollover() {
    try {
      closeFile()
      moveFile()
      openFile()
      if (maxRetainedFiles > 0) {
        deleteOldFiles()
      }
    } catch {
      case e: Exception =>
        logError(s"Error rolling over $activeFile", e)
    }
  }

  /** Move the active log file to a new rollover file */
  /** 移动这个活动的日志文件到一个新的翻转文件 */
  private def moveFile() {
    val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
    val rolloverFile = new File(
      activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
    try {
      logDebug(s"Attempting to rollover file $activeFile to file $rolloverFile")
      if (activeFile.exists) {
        if (!rolloverFile.exists) {
          Files.move(activeFile, rolloverFile)
          logInfo(s"Rolled over $activeFile to $rolloverFile")
        } else {
          // In case the rollover file name clashes, make a unique file name.
          // The resultant file names are long and ugly, so this is used only
          // if there is a name collision. This can be avoided by the using
          // the right pattern such that name collisions do not occur.
          // 一旦这个翻转文件名字冲突 , 产生一个独一无二的文件名.
          // 因为这些组合的文件名是长且丑陋, 因此只有当有一个名字冲突出现的时候才适合使用
          // 这个采用使用适当的模式来避免名称冲突的发生
          var i = 0
          var altRolloverFile: File = null
          do {
            altRolloverFile = new File(activeFile.getParent,
              s"${activeFile.getName}$rolloverSuffix--$i").getAbsoluteFile
            i += 1
          } while (i < 10000 && altRolloverFile.exists)

          logWarning(s"Rollover file $rolloverFile already exists, " +
            s"rolled over $activeFile to file $altRolloverFile")
          Files.move(activeFile, altRolloverFile)
        }
      } else {
        logWarning(s"File $activeFile does not exist")
      }
    }
  }

  /** Retain only last few files */
  /** 只保留最近的少量文件(就是删掉老的文件) */
  private[util] def deleteOldFiles() {
    try {
      val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
        def accept(f: File): Boolean = {
          f.getName.startsWith(activeFile.getName) && f != activeFile
        }
      }).sorted
      val filesToBeDeleted = rolledoverFiles.take(
        math.max(0, rolledoverFiles.size - maxRetainedFiles))
      filesToBeDeleted.foreach { file =>
        logInfo(s"Deleting file executor log file ${file.getAbsolutePath}")
        file.delete()
      }
    } catch {
      case e: Exception =>
        logError("Error cleaning logs in directory " + activeFile.getParentFile.getAbsolutePath, e)
    }
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.RollingFileAppender]]. Defines
 * names of configurations that configure rolling file appenders.
 */
/**
 * [[org.apache.spark.util.logging.RollingFileAppender]]类的伴生对象.定义了
 * 可以配置rolling file appenders的若干配置名称。
 */
private[spark] object RollingFileAppender {
  val STRATEGY_PROPERTY = "spark.executor.logs.rolling.strategy"
  val STRATEGY_DEFAULT = ""
  val INTERVAL_PROPERTY = "spark.executor.logs.rolling.time.interval"
  val INTERVAL_DEFAULT = "daily"
  val SIZE_PROPERTY = "spark.executor.logs.rolling.size.maxBytes"
  val SIZE_DEFAULT = (1024 * 1024).toString
  val RETAINED_FILES_PROPERTY = "spark.executor.logs.rolling.maxRetainedFiles"
  val DEFAULT_BUFFER_SIZE = 8192

  /**
   * Get the sorted list of rolled over files. This assumes that the all the rolled
   * over file names are prefixed with the `activeFileName`, and the active file
   * name has the latest logs. So it sorts all the rolled over logs (that are
   * prefixed with `activeFileName`) and appends the active file
   */
  /**
   * 获取排序后的已翻转的文件列表.这里假定所有的翻转过的文件名称都以`activeFileName`
   * 作为前缀 , 而且这个活动的文件名具有这个最近的日志. 那么这个方法排序翻转文件日志 (以
   * `activeFileName`作为前缀) 并且追加到这个活动日志文件之后
   */
  def getSortedRolledOverFiles(directory: String, activeFileName: String): Seq[File] = {
    val rolledOverFiles = new File(directory).getAbsoluteFile.listFiles.filter { file =>
      val fileName = file.getName
      fileName.startsWith(activeFileName) && fileName != activeFileName
    }.sorted
    val activeFile = {
      val file = new File(directory, activeFileName).getAbsoluteFile
      if (file.exists) Some(file) else None
    }
    rolledOverFiles ++ activeFile
  }
}
