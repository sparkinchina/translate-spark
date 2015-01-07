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

package org.apache.spark.network

import java.io.Closeable
import java.nio.ByteBuffer

import scala.concurrent.{Promise, Await, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.Logging
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{ShuffleClient, BlockFetchingListener}
import org.apache.spark.storage.{BlockManagerId, BlockId, StorageLevel}

private[spark]
abstract class BlockTransferService extends ShuffleClient with Closeable with Logging {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   */
  /**
   * 通过给它提供BlockDataManager来初始化传输服务，可以用来获取本地块或将放置本地块。
   */
  def init(blockDataManager: BlockDataManager)

  /**
   * Tear down the transfer service.
   */
  /**
   *  关闭这个传输服务..
   */
  def close(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  /**
   * 当前这个服务监听的端口, 只有当 [[init]]被调用后可用.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  /**
   * 当前这个服务监听的主机名, 只有当 [[init]]被调用后可用.
   */
  def hostName: String

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  /**
   * 单数据块上传到远程节点，只有在[[init]]被调用后可用.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  /**
   * 一个 [[fetchBlocks]]的特殊用例,它只获取一个数据块并且置于阻塞中.
   *
   * 只有当 [[init]] 被调用后才可用.
   */
  def fetchBlockSync(host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
    // A monitor for the thread to wait on.
    // 为了处理等待的线程的一个监视器.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          val ret = ByteBuffer.allocate(data.size.toInt)
          ret.put(data.nioByteBuffer())
          ret.flip()
          result.success(new NioManagedBuffer(ret))
        }
      })

    Await.result(result.future, Duration.Inf)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   */
  /**
   * 单数据块上传到远程节点，只有在[[init]]被调用后可用..
   *
   * 这个方法和 [[uploadBlock]]类似, 不同的是这个方法会阻塞线程直到上传完成(同步).
   */
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Unit = {
    Await.result(uploadBlock(hostname, port, execId, blockId, blockData, level), Duration.Inf)
  }
}
