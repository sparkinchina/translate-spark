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

import org.apache.spark.scheduler.MapStatus

/**
 * Obtained inside a map task to write out records to the shuffle system.
 */
private[spark] trait ShuffleWriter[K, V] {
  /** Write a bunch of records to this task's output */
  /** 将一大堆的记录写到当前任务的输出中 */
  def write(records: Iterator[_ <: Product2[K, V]]): Unit

  /** Close this writer, passing along whether the map completed */
  /** 关闭这个writer, 传回是否这个map计算过程完成的状态 */
  def stop(success: Boolean): Option[MapStatus]
}
