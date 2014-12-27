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

package org.apache.spark.util.collection

import org.apache.spark.Logging
import org.apache.spark.SparkEnv

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
/**
 * 当这个内存使用已经超出阈值时将一个内存中集合的内容溢出写到磁盘
 */
private[spark] trait Spillable[C] extends Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  /**
   * 溢出当前内存中集合内容到磁盘，并释放这块内存..
   *
   * @param collection 溢出到磁盘的集合
   */
  protected def spill(collection: C): Unit

  // Number of elements read from input since last spill
  // 自从最近一次溢出后从输入中读取的元素的数量
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  // 每当一个记录被读取时会被子类调用
  // 这个方法用来检查溢出频率
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Memory manager that can be used to acquire/release memory
  // 可以用来获取/释放内存得内存管理器
  private[this] val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  // Threshold for `elementsRead` before we start tracking this collection's memory usage
  // 在我们开始跟踪这个集合的内存使用前的 `elementsRead` 的阈值
  private[this] val trackMemoryThreshold = 1000

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // Exposed for testing
  // 在我们开始跟踪这个集合的内存使用量前限制一个集合的容量的初始阈值
  // 为了测试而暴露这个值（spark.shuffle.spill.initialMemoryThreshold）
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  // 在我们开始跟踪这个内存使用量之前的一个集合容量的阈值，用来避免大量的小数据溢出，按照级数
  // 大于零的重要性顺序初始化这个值
  private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  // 自从最后一次溢出后从输入读取的元素数
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  // 总共的溢出字节数
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  // 溢出的数量
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  /**
   * 如有必要就将当前的内存中集合溢出到磁盘. 在溢出之前会尝试获取更多的内存.
   *
   * @param collection 溢出到磁盘的集合
   * @param currentMemory 按照字节数估算的内存大小
   * @return 如果`collection`被溢出到磁盘返回true,否则返回false
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    if (elementsRead > trackMemoryThreshold && elementsRead % 32 == 0 &&
        currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      // 请求从当前的shuffle内存池中将我们的当前内存加倍
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)
      myMemoryThreshold += granted
      if (myMemoryThreshold <= currentMemory) {
        // We were granted too little memory to grow further (either tryToAcquire returned 0,
        // or we already had more memory than myMemoryThreshold); spill the current collection
        // 我们已假定太小内存会进一步增长 (要么 tryToAcquire 返回 0,
        // 要么我们已经拥有比myMemoryThreshold更多的内存); 溢出当前的集合
        _spillCount += 1
        logSpillage(currentMemory)

        spill(collection)

        _elementsRead = 0
        // Keep track of spills, and release memory
        // 跟踪这个溢出，并且释放内存
        _memoryBytesSpilled += currentMemory
        releaseMemoryForThisThread()
        return true
      }
    }
    false
  }

  /**
   * @return number of bytes spilled in total
   */
  /**
   * @return 合计溢出的字节数
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the shuffle pool so that other threads can grab it.
   */
  /**
   * 将我们的内存释放回shuffle池以便其他线程可以抢占.
   */
  private def releaseMemoryForThisThread(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    shuffleMemoryManager.release(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  /**
   * 打印输出一个标准的日志消息-详细溢出量.
   *
   * @param size 溢出的字节数
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
