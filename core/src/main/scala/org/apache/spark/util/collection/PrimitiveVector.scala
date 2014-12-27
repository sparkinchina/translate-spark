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
 * An append-only, non-threadsafe, array-backed vector that is optimized for primitive types.
 */
/**
 * 一个为元素类型优化的只追加、非线程安全且基于数组的向量.
 */
private[spark]
class PrimitiveVector[@specialized(Long, Int, Double) V: ClassTag](initialSize: Int = 64) {
  private var _numElements = 0
  private var _array: Array[V] = _

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size.
  // NB: 这个必须从声明分离, 否则这个特定的父类将会得到伴随这同样初始大小的它自身的数组
  // （译者注：开发者不希望父类在构造时直接能得到同样初始容量的数组）。
  _array = new Array[V](initialSize)

  def apply(index: Int): V = {
    require(index < _numElements)
    _array(index)
  }

  def +=(value: V): Unit = {
    if (_numElements == _array.length) {
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  def capacity: Int = _array.length

  def length: Int = _numElements

  def size: Int = _numElements

  def iterator: Iterator[V] = new Iterator[V] {
    var index = 0
    override def hasNext: Boolean = index < _numElements
    override def next(): V = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = _array(index)
      index += 1
      value
    }
  }

  /** Gets the underlying array backing this vector. */
  /** 获得这个支持当前向量的低层数组. */
  def array: Array[V] = _array

  /** Trims this vector so that the capacity is equal to the size. */
  /** 剪裁这个vector以便这个容量和大小相等. */
  def trim(): PrimitiveVector[V] = resize(size)

  /** Resizes the array, dropping elements if the total length decreases. */
  /** 调整数组大小, 如果当前总长度减少移除若干元素. */
  def resize(newLength: Int): PrimitiveVector[V] = {
    val newArray = new Array[V](newLength)
    _array.copyToArray(newArray)
    _array = newArray
    if (newLength < _numElements) {
      _numElements = newLength
    }
    this
  }
}
