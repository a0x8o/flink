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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
<<<<<<< HEAD
=======
import org.apache.flink.runtime.concurrent.Executors;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the CurrentJobIdsHandler.
 */
public class CurrentJobIdsHandlerTest {
	@Test
	public void testGetPaths() {
<<<<<<< HEAD
		CurrentJobIdsHandler handler = new CurrentJobIdsHandler(Time.seconds(0L));
=======
		CurrentJobIdsHandler handler = new CurrentJobIdsHandler(Executors.directExecutor(), Time.seconds(0L));
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs", paths[0]);
	}
}
