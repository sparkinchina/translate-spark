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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, TaskContext, ShuffleDependency}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.hash.HashShuffleReader

private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager {

  private val indexShuffleBlockManager = new IndexShuffleBlockManager(conf)
  private val shuffleMapNumber = new ConcurrentHashMap[Int, Int]()

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  /**
   * 通过这个管理者注册一个shuffle并且为他获取一个处理器然后传给任务.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  /**
   * 给reduce分区的一个范围获取一个reader (startPartition 到 endPartition-1, 排除的).
   * 由reduce任务在执行者上调用.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    // We currently use the same block store shuffle fetcher as the hash-based shuffle.
    // 我们目前使用的同一块存储访问作为一个基于hash shuffle的shuffle获取器.
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  /** 为一个给定的分区获取一个writer. 被map任务在执行器上调用. */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    shuffleMapNumber.putIfAbsent(baseShuffleHandle.shuffleId, baseShuffleHandle.numMaps)
    new SortShuffleWriter(
      shuffleBlockManager, baseShuffleHandle, mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  /** 从ShuffleManager中溢出一个shuffle的元数据块. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (shuffleMapNumber.containsKey(shuffleId)) {
      val numMaps = shuffleMapNumber.remove(shuffleId)
      (0 until numMaps).map{ mapId =>
        shuffleBlockManager.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def shuffleBlockManager: IndexShuffleBlockManager = {
    indexShuffleBlockManager
  }

  /** Shut down this ShuffleManager. */
  /** 关闭当前这个ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockManager.stop()
  }
}
