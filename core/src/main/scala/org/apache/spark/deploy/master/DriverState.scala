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

package org.apache.spark.deploy.master

private[spark] object DriverState extends Enumeration {

  type DriverState = Value

  // SUBMITTED: Submitted but not yet scheduled on a worker
  // RUNNING: Has been allocated to a worker to run
  // FINISHED: Previously ran and exited cleanly
  // RELAUNCHING: Exited non-zero or due to worker failure, but has not yet started running again
  // UNKNOWN: The state of the driver is temporarily not known due to master failure recovery
  // KILLED: A user manually killed this driver
  // FAILED: The driver exited non-zero and was not supervised
  // ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)
  /**
   * DriverState状态
   * SUBMITTED: 已经提交到Master，但Master还没有调度到Worker上
   * RUNNING: Master已经分配了Worker节点，用于执行Driver
   * FINISHED: 上一次运行正常，干净退出
   * RELAUNCHING: 退出的错误代码不是0 或者由于Worker节点失败，被重新调度但还没有再次运行的状态
   * UNKNOWN: 由于Master节点失败处于恢复阶段导致的Driver状态不可知
   * KILLED: Driver被用户手动杀死
   * FAILED: Driver退出代码非0 ，并且没有监管者
   * ERROR: 由于不可恢复的错误（如Jar文件缺失）导致Driver不能运行或者重新运行
   */
  val SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR = Value
}
