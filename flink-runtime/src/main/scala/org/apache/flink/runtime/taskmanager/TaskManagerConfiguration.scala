/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskmanager

import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration

import scala.concurrent.duration.FiniteDuration

case class TaskManagerConfiguration(
    tmpDirPaths: Array[String],
    cleanupInterval: Long,
    timeout: FiniteDuration,
    maxRegistrationDuration: Option[FiniteDuration],
    numberOfSlots: Int,
    configuration: Configuration,
    initialRegistrationPause: FiniteDuration,
    maxRegistrationPause: FiniteDuration,
    refusedRegistrationPause: FiniteDuration) {

  def this(
      tmpDirPaths: Array[String],
      cleanupInterval: Long,
      timeout: FiniteDuration,
      maxRegistrationDuration: Option[FiniteDuration],
      numberOfSlots: Int,
      configuration: Configuration) {
    this (
      tmpDirPaths,
      cleanupInterval,
      timeout,
      maxRegistrationDuration,
      numberOfSlots,
      configuration,
      FiniteDuration(500, TimeUnit.MILLISECONDS),
      FiniteDuration(30, TimeUnit.SECONDS),
      FiniteDuration(10, TimeUnit.SECONDS))
  }
}
