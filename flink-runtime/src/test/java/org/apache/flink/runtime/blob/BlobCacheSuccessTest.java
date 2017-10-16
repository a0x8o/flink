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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
<<<<<<< HEAD
=======
import org.apache.flink.util.Preconditions;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
<<<<<<< HEAD
import java.util.Arrays;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
=======
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

/**
 * This class contains unit tests for the {@link BlobCacheService}.
 */
public class BlobCacheSuccessTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * BlobCache with no HA, job-unrelated BLOBs. BLOBs need to be downloaded form a working
<<<<<<< HEAD
	 * BlobServer.
	 */
	@Test
	public void testBlobNoJobCache() throws IOException {
=======
	 * BlobServer.
	 */
	@Test
	public void testBlobNoJobCache() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		uploadFileGetTest(config, null, false, false);
	}

	/**
	 * BlobCache with no HA, job-related BLOBS. BLOBs need to be downloaded form a working
	 * BlobServer.
	 */
	@Test
	public void testBlobForJobCache() throws IOException {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

<<<<<<< HEAD
		uploadFileGetTest(config, null, false, false, TRANSIENT_BLOB);
	}

	/**
	 * BlobCache with no HA, job-related BLOBS. BLOBs need to be downloaded form a working
	 * BlobServer.
	 */
	@Test
	public void testBlobForJobCache() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		uploadFileGetTest(config, new JobID(), false, false, TRANSIENT_BLOB);
=======
		uploadFileGetTest(config, new JobID(), false, false);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
<<<<<<< HEAD
	 * BlobServer which remains active after the BLOB upload. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa() throws IOException {
=======
	 * BlobServer. Using job-unrelated BLOBs.
	 */
	@Test
	public void testBlobNoJobCacheHa() throws IOException {
		testBlobCacheHa(null);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa() throws IOException {
		testBlobCacheHa(new JobID());
	}

	private void testBlobCacheHa(final JobID jobId) throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
		uploadFileGetTest(config, jobId, true, true);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-unrelated BLOBs.
	 */
	@Test
	public void testBlobNoJobCacheHa2() throws IOException {
		testBlobCacheHa2(null);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa2() throws IOException {
		testBlobCacheHa2(new JobID());
	}

	private void testBlobCacheHa2(JobID jobId) throws IOException {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
<<<<<<< HEAD

		uploadFileGetTest(config, new JobID(), true, true, PERMANENT_BLOB);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer which is shut down after the BLOB upload. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa2() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
		uploadFileGetTest(config, new JobID(), false, true, PERMANENT_BLOB);
=======
		uploadFileGetTest(config, jobId, false, true);
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer. Using job-unrelated BLOBs.
	 */
	@Test
	public void testBlobNoJobCacheHaFallback() throws IOException {
		testBlobCacheHaFallback(null);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHaFallback() throws IOException {
<<<<<<< HEAD
=======
		testBlobCacheHaFallback(new JobID());
	}

	private void testBlobCacheHaFallback(final JobID jobId) throws IOException {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
<<<<<<< HEAD

		uploadFileGetTest(config, new JobID(), false, false, PERMANENT_BLOB);
=======
		uploadFileGetTest(config, jobId, false, false);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	/**
	 * Uploads two different BLOBs to the {@link BlobServer} via a {@link BlobClient} and verifies
<<<<<<< HEAD
	 * we can access the files from a {@link BlobCacheService}.
=======
	 * we can access the files from a {@link BlobCache}.
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	 *
	 * @param config
	 * 		configuration to use for the server and cache (the final cache's configuration will
	 * 		actually get some modifications)
	 * @param shutdownServerAfterUpload
	 * 		whether the server should be shut down after uploading the BLOBs (only useful with HA mode)
	 * 		- this implies that the cache has access to the shared <tt>HA_STORAGE_PATH</tt>
	 * @param cacheHasAccessToFs
	 * 		whether the cache should have access to a shared <tt>HA_STORAGE_PATH</tt> (only useful with
	 * 		HA mode)
<<<<<<< HEAD
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 */
	private void uploadFileGetTest(
			final Configuration config, @Nullable JobID jobId, boolean shutdownServerAfterUpload,
			boolean cacheHasAccessToFs, BlobKey.BlobType blobType) throws IOException {

		final Configuration cacheConfig = new Configuration(config);
		cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		if (!cacheHasAccessToFs) {
			// make sure the cache cannot access the HA store directly
			cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			cacheConfig.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
				temporaryFolder.newFolder().getPath() + "/does-not-exist");
		}
=======
	 */
	private void uploadFileGetTest(final Configuration config, JobID jobId, boolean shutdownServerAfterUpload,
			boolean cacheHasAccessToFs) throws IOException {
		Preconditions.checkArgument(!shutdownServerAfterUpload || cacheHasAccessToFs);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		// First create two BLOBs and upload them to BLOB server
		final byte[] data = new byte[128];
		byte[] data2 = Arrays.copyOf(data, data.length);
		data2[0] ^= 1;

		BlobStoreService blobStoreService = null;
<<<<<<< HEAD
=======
		try {
			final Configuration cacheConfig = new Configuration(config);
			cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			if (!cacheHasAccessToFs) {
				// make sure the cache cannot access the HA store directly
				cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
					temporaryFolder.newFolder().getAbsolutePath());
				cacheConfig.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
					temporaryFolder.newFolder().getPath() + "/does-not-exist");
			}
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(cacheConfig);
			try (
				BlobServer server = new BlobServer(config, blobStoreService);
				BlobCacheService cache = new BlobCacheService(new InetSocketAddress("localhost", server.getPort()),
					cacheConfig, blobStoreService)) {

				server.start();

				// Upload BLOBs
				BlobKey key1 = put(server, jobId, data, blobType);
				BlobKey key2 = put(server, jobId, data2, blobType);

<<<<<<< HEAD
				if (shutdownServerAfterUpload) {
					// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
					server.close();
				}
=======
				blobKeys.add(blobClient.put(jobId, buf));
				buf[0] = 1; // Make sure the BLOB key changes
				blobKeys.add(blobClient.put(jobId, buf));
			} finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			if (shutdownServerAfterUpload) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.close();
				blobServer = null;
			}
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

				verifyContents(cache, jobId, key1, data);
				verifyContents(cache, jobId, key2, data2);

<<<<<<< HEAD
				if (shutdownServerAfterUpload) {
					// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
					server.close();

					verifyContents(cache, jobId, key1, data);
					verifyContents(cache, jobId, key2, data2);
				}
=======
			for (BlobKey blobKey : blobKeys) {
				if (jobId == null) {
					blobCache.getFile(blobKey);
				} else {
					blobCache.getFile(jobId, blobKey);
				}
			}

			if (blobServer != null) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.close();
				blobServer = null;
			}

			final File[] files = new File[blobKeys.size()];

			for(int i = 0; i < blobKeys.size(); i++){
				if (jobId == null) {
					files[i] = blobCache.getFile(blobKeys.get(i));
				} else {
					files[i] = blobCache.getFile(jobId, blobKeys.get(i));
				}
			}

			// Verify the result
			assertEquals(blobKeys.size(), files.length);

			for (final File file : files) {
				assertNotNull(file);

				assertTrue(file.exists());
				assertEquals(buf.length, file.length());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			}
		} finally {
			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}
}
