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

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 *
 * 为了让Master从一次事故中恢复，让Master保存其状态是必须的环节，这里PersistenceEngine包含了两次含义：
 *  --- 一个新的App或者Worker完成注册前必须调用 addApplication 或者 addWorker
 *  --- 在任何时候都能调用 removeApplication 或者 removeWorker
 *  满足以上两条，我们就能让所以的App和Worker进行持久化，但也可能Job完成后，我们还没有来得及删除App或者Worker,
 *  failure就发生了。因此他们的生存与否必须在恢复时进行必要的验证
 *
 */
private[spark] trait PersistenceEngine {
  def addApplication(app: ApplicationInfo)

  def removeApplication(app: ApplicationInfo)

  def addWorker(worker: WorkerInfo)

  def removeWorker(worker: WorkerInfo)

  def addDriver(driver: DriverInfo)

  def removeDriver(driver: DriverInfo)

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo])

  def close() {}
}

private[spark] class BlackHolePersistenceEngine extends PersistenceEngine {
  override def addApplication(app: ApplicationInfo) {}
  override def removeApplication(app: ApplicationInfo) {}
  override def addWorker(worker: WorkerInfo) {}
  override def removeWorker(worker: WorkerInfo) {}
  override def addDriver(driver: DriverInfo) {}
  override def removeDriver(driver: DriverInfo) {}

  override def readPersistedData() = (Nil, Nil, Nil)
}
