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

package org.apache.spark.broadcast

import java.io.Serializable

import org.apache.spark.SparkException
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
 * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
 * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
 * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
 * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
 * communication cost.
 *
 * Broadcast variables are created from a variable `v` by calling
 * [[org.apache.spark.SparkContext#broadcast]].
 * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
 * `value` method. The interpreter session below shows this:
 *
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 * After the broadcast variable is created, it should be used instead of the value `v` in any
 * functions run on the cluster so that `v` is not shipped to the nodes more than once.
 * In addition, the object `v` should not be modified after it is broadcast in order to ensure
 * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
 * to a new node later).
 *
 * Broadcast 变量可以让程序在每个节点机器保存一份只读变量而不是在每个任务打包一份。比如，可以使用Broadcast变量
 * 高效地为每个节点提供一个巨大输入的数据集的副本。Spark也会尝试使用一些高效的算法来分发Broadcast变量以期减少通讯成本。
 *
 * Broadcast 变量是通过对一个普通变量 `v` 调用 [[org.apache.spark.SparkContext#broadcast]] 创建出来的
 * Broadcast 变量就是对普通变量 `v` 的一个包装, 你可以通过 `value` 方法来访问 `v` 的值。示例如上。
 *
 * 创建Broadcast变量之后，任何在集群上执行的函数都应该使用该Broadcast变量而不是 `v`，如此 `v`就不会多次传递到节点上了
 * 此外，对象 `v`的值在被广播后就不能修改，这样才能保证所有节点上数据的一致性
 *
 * `value` method. The interpreter session below shows this:
 * @param id A unique identifier for the broadcast variable.
 * @tparam T Type of the data contained in the broadcast variable.
 */

abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * Flag signifying whether the broadcast variable is valid
   * (that is, not already destroyed) or not.
   */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** Get the broadcasted value. */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   * Asynchronously delete cached copies of this broadcast on the executors.
   * If the broadcast is used after this is called, it will need to be re-sent to each executor.
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   * Delete cached copies of this broadcast on the executors. If the broadcast is used after
   * this is called, it will need to be re-sent to each executor.
   * @param blocking Whether to block until unpersisting has completed
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * This method blocks until destroy has completed
   */
  def destroy() {
    destroy(blocking = true)
  }

  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * @param blocking Whether to block until destroy has completed
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    doDestroy(blocking)
  }

  /**
   * Whether this Broadcast is actually usable. This should be false once persisted state is
   * removed from the driver.
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * Actually get the broadcasted value. Concrete implementations of Broadcast class must
   * define their own way to get the value.
   */
  protected def getValue(): T

  /**
   * Actually unpersist the broadcasted value on the executors. Concrete implementations of
   * Broadcast class must define their own logic to unpersist their own data.
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * Actually destroy all data and metadata related to this broadcast variable.
   * Implementation of Broadcast class must define their own logic to destroy their own
   * state.
   */
  protected def doDestroy(blocking: Boolean)

  /** Check if this broadcast is valid. If not valid, exception is thrown. */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  override def toString = "Broadcast(" + id + ")"
}
