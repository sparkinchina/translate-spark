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

package org.apache.spark.shuffle

/**
 * Obtained inside a reduce task to read combined records from the mappers.
 */
/**
 * 在一个reduce任务中获取来读取来源于mappers的组合记录
 */
private[spark] trait ShuffleReader[K, C] {
  /** Read the combined key-values for this reduce task */
  /** 为了这个reduce任务读取这个组合键值集 */
  def read(): Iterator[Product2[K, C]]

  /**
   * Close this reader.
   * TODO: Add this back when we make the ShuffleReader a developer API that others can implement
   * (at which point this will likely be necessary).
   */
  /**
   * 关闭当前reader.
   * TODO: 这个时候让ShuffleReader这个开发者API 其他人能够实现
   * (在这一点上，这可能是必要的).
   */
  // def stop(): Unit
}
