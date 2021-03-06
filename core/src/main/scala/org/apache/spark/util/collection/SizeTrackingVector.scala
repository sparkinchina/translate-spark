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

import scala.reflect.ClassTag

/**
 * An append-only buffer that keeps track of its estimated size in bytes.
 */
/**
 * 一个跟踪目标估算字节容量的只追加的缓冲区.
 */
private[spark] class SizeTrackingVector[T: ClassTag]
  extends PrimitiveVector[T]
  with SizeTracker {

  override def +=(value: T): Unit = {
    super.+=(value)
    super.afterUpdate()
  }

  override def resize(newLength: Int): PrimitiveVector[T] = {
    super.resize(newLength)
    resetSamples()
    this
  }

  /**
   * Return a trimmed version of the underlying array.
   */
  /**
   * 返回一个底层数组的剪裁版本
   */
  def toArray: Array[T] = {
    super.iterator.toArray
  }
}
