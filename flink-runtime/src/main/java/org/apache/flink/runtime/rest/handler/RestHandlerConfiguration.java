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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.Preconditions;

import java.io.File;

/**
 * Configuration object containing values for the rest handler configuration.
 */
public class RestHandlerConfiguration {

	private final long refreshInterval;

<<<<<<< HEAD
	private final int maxCheckpointStatisticCacheEntries;

=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	private final Time timeout;

	private final File tmpDir;

<<<<<<< HEAD
	public RestHandlerConfiguration(
			long refreshInterval,
			int maxCheckpointStatisticCacheEntries,
			Time timeout,
			File tmpDir) {
		Preconditions.checkArgument(refreshInterval > 0L, "The refresh interval (ms) should be larger than 0.");
		this.refreshInterval = refreshInterval;

		this.maxCheckpointStatisticCacheEntries = maxCheckpointStatisticCacheEntries;

=======
	public RestHandlerConfiguration(long refreshInterval, Time timeout, File tmpDir) {
		Preconditions.checkArgument(refreshInterval > 0L, "The refresh interval (ms) should be larger than 0.");
		this.refreshInterval = refreshInterval;

>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		this.timeout = Preconditions.checkNotNull(timeout);
		this.tmpDir = Preconditions.checkNotNull(tmpDir);
	}

	public long getRefreshInterval() {
		return refreshInterval;
	}

<<<<<<< HEAD
	public int getMaxCheckpointStatisticCacheEntries() {
		return maxCheckpointStatisticCacheEntries;
	}

=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	public Time getTimeout() {
		return timeout;
	}

	public File getTmpDir() {
		return tmpDir;
	}

	public static RestHandlerConfiguration fromConfiguration(Configuration configuration) {
		final long refreshInterval = configuration.getLong(WebOptions.REFRESH_INTERVAL);

<<<<<<< HEAD
		final int maxCheckpointStatisticCacheEntries = configuration.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

=======
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		final File tmpDir = new File(configuration.getString(WebOptions.TMP_DIR));

<<<<<<< HEAD
		return new RestHandlerConfiguration(refreshInterval, maxCheckpointStatisticCacheEntries, timeout, tmpDir);
=======
		return new RestHandlerConfiguration(refreshInterval, timeout, tmpDir);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}
}
