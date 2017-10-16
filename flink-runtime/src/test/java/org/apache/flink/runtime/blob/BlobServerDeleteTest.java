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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
<<<<<<< HEAD

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
=======

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFileCountForJob;
<<<<<<< HEAD
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
=======
import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFilesExist;
import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests how DELETE requests behave.
 */
public class BlobServerDeleteTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
<<<<<<< HEAD

	@Test
	public void testDeleteTransient1() throws IOException {
		testDeleteTransient(null, new JobID());
	}

	@Test
	public void testDeleteTransient2() throws IOException {
		testDeleteTransient(new JobID(), null);
	}

	@Test
	public void testDeleteTransient3() throws IOException {
		testDeleteTransient(null, null);
	}

	@Test
	public void testDeleteTransient4() throws IOException {
		testDeleteTransient(new JobID(), new JobID());
	}

	/**
	 * Uploads a (different) byte array for each of the given jobs and verifies that deleting one of
	 * them (via the {@link BlobServer}) does not influence the other.
	 *
	 * @param jobId1
	 * 		first job id
	 * @param jobId2
	 * 		second job id
	 */
	private void testDeleteTransient(@Nullable JobID jobId1, @Nullable JobID jobId2)
			throws IOException {
		final boolean sameJobId = (jobId1 == jobId2) || (jobId1 != null && jobId1.equals(jobId2));

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOf(data, data.length);
			data2[0] ^= 1;

			// put first BLOB
			TransientBlobKey key1 = (TransientBlobKey) put(server, jobId1, data, TRANSIENT_BLOB);
			assertNotNull(key1);

			// put two more BLOBs (same key, other key) for another job ID
			TransientBlobKey key2a = (TransientBlobKey) put(server, jobId2, data, TRANSIENT_BLOB);
			assertNotNull(key2a);
			assertEquals(key1, key2a);
			TransientBlobKey key2b = (TransientBlobKey) put(server, jobId2, data2, TRANSIENT_BLOB);
			assertNotNull(key2b);

			// issue a DELETE request
			assertTrue(delete(server, jobId1, key1));

			verifyDeleted(server, jobId1, key1);
			// deleting a one BLOB should not affect another BLOB, even with the same key if job IDs are different
			if (!sameJobId) {
				verifyContents(server, jobId2, key2a, data);
			}
			verifyContents(server, jobId2, key2b, data2);

			// delete first file of second job
			assertTrue(delete(server, jobId2, key2a));
			verifyDeleted(server, jobId2, key2a);
			verifyContents(server, jobId2, key2b, data2);

			// delete second file of second job
			assertTrue(delete(server, jobId2, key2b));
			verifyDeleted(server, jobId2, key2b);
		}
	}

	@Test
	public void testDeleteTransientAlreadyDeletedNoJob() throws IOException {
		testDeleteTransientAlreadyDeleted(null);
	}

	@Test
	public void testDeleteTransientAlreadyDeletedForJob() throws IOException {
		testDeleteTransientAlreadyDeleted(new JobID());
	}

	/**
	 * Uploads a byte array for the given job and verifies that deleting it (via the {@link
	 * BlobServer}) does not fail independent of whether the file exists.
	 *
	 * @param jobId
	 * 		job id
	 */
	private void testDeleteTransientAlreadyDeleted(@Nullable final JobID jobId) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
=======

	@Test
	public void testDeleteSingleByBlobKey() throws IOException {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put job-unrelated (like libraries)
			BlobKey key1 = client.put(null, data);
			assertNotNull(key1);

			// second job-unrelated item
			data[0] ^= 1;
			BlobKey key2 = client.put(null, data);
			assertNotNull(key2);
			assertNotEquals(key1, key2);

			// put job-related with same key1 as non-job-related
			data[0] ^= 1; // back to the original data
			final JobID jobId = new JobID();
			BlobKey key1b = client.put(jobId, data);
			assertNotNull(key1b);
			assertEquals(key1, key1b);

			// issue a DELETE request via the client
			client.delete(key1);
			client.close();

			client = new BlobClient(serverAddress, config);
			try (InputStream ignored = client.get(key1)) {
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}

			ensureClientIsClosed(client);

			client = new BlobClient(serverAddress, config);
			try {
				// NOTE: the server will stall in its send operation until either the data is fully
				//       read or the socket is closed, e.g. via a client.close() call
				validateGetAndClose(client.get(jobId, key1), data);
			}
			catch (IOException e) {
				fail("Deleting a job-unrelated BLOB should not affect a job-related BLOB with the same key");
			}
			client.close();

			// delete a file directly on the server
			server.delete(key2);
			try {
				server.getFile(key2);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}
		}
		finally {
			cleanup(server, client);
		}
	}

	private static void ensureClientIsClosed(final BlobClient client) throws IOException {
		try {
			client.put(null, new byte[1]);
			fail("client should be closed after erroneous operation");
		}
		catch (IllegalStateException e) {
			// expected
		} finally {
			client.close();
		}
	}

	@Test
	public void testDeleteAlreadyDeletedNoJob() throws IOException {
		testDeleteAlreadyDeleted(null);
	}

	@Test
	public void testDeleteAlreadyDeletedForJob() throws IOException {
		testDeleteAlreadyDeleted(new JobID());
	}

	private void testDeleteAlreadyDeleted(final JobID jobId) throws IOException {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

<<<<<<< HEAD
			// put BLOB
			TransientBlobKey key = (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
=======
			// put file
			BlobKey key = client.put(jobId, data);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
			assertNotNull(key);

			File blobFile = server.getStorageLocation(jobId, key);
			assertTrue(blobFile.delete());

<<<<<<< HEAD
			// DELETE operation should not fail if file is already deleted
			assertTrue(delete(server, jobId, key));
			verifyDeleted(server, jobId, key);

			// one more delete call that should not fail
			assertTrue(delete(server, jobId, key));
			verifyDeleted(server, jobId, key);
		}
	}

	@Test
	public void testDeleteTransientFailsNoJob() throws IOException {
		testDeleteTransientFails(null);
	}

	@Test
	public void testDeleteTransientFailsForJob() throws IOException {
		testDeleteTransientFails(new JobID());
	}

	/**
	 * Uploads a byte array for the given job and verifies that a delete operation (via the {@link
	 * BlobServer}) does not fail even if the file is not deletable, e.g. via restricting the
	 * permissions.
	 *
	 * @param jobId
	 * 		job id
	 */
	private void testDeleteTransientFails(@Nullable final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File blobFile = null;
		File directory = null;

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			try {
				byte[] data = new byte[2000000];
				rnd.nextBytes(data);

				// put BLOB
				TransientBlobKey key = (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
				assertNotNull(key);

				blobFile = server.getStorageLocation(jobId, key);
				directory = blobFile.getParentFile();

				assertTrue(blobFile.setWritable(false, false));
				assertTrue(directory.setWritable(false, false));

				// issue a DELETE request
				assertFalse(delete(server, jobId, key));

				// the file should still be there
				verifyContents(server, jobId, key, data);
			} finally {
				if (blobFile != null && directory != null) {
					//noinspection ResultOfMethodCallIgnored
					blobFile.setWritable(true, false);
					//noinspection ResultOfMethodCallIgnored
					directory.setWritable(true, false);
				}
			}
=======
			// issue a DELETE request via the client
			try {
				deleteHelper(client, jobId, key);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}

			// issue a DELETE request on the server
			if (jobId == null) {
				server.delete(key);
			} else {
				server.delete(jobId, key);
			}
		}
		finally {
			cleanup(server, client);
		}
	}

	private static void deleteHelper(BlobClient client, JobID jobId, BlobKey key) throws IOException {
		if (jobId == null) {
			client.delete(key);
		} else {
			client.delete(jobId, key);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		}
	}

	@Test
<<<<<<< HEAD
	public void testJobCleanup() throws IOException, InterruptedException {
		testJobCleanup(TRANSIENT_BLOB);
	}

	@Test
	public void testJobCleanupHa() throws IOException, InterruptedException {
		testJobCleanup(PERMANENT_BLOB);
	}

	/**
	 * Tests that {@link BlobServer} cleans up after calling {@link BlobServer#cleanupJob(JobID)}.
	 *
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 */
	private void testJobCleanup(BlobKey.BlobType blobType) throws IOException {
		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();
=======
	public void testDeleteFailsNoJob() throws IOException {
		testDeleteFails(null);
	}

	@Test
	public void testDeleteFailsForJob() throws IOException {
		testDeleteFails(new JobID());
	}

	private void testDeleteFails(final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		File blobFile = null;
		File directory = null;
		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(jobId, data);
			assertNotNull(key);

			blobFile = server.getStorageLocation(jobId, key);
			directory = blobFile.getParentFile();
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

<<<<<<< HEAD
		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			final byte[] data = new byte[128];
			byte[] data2 = Arrays.copyOf(data, data.length);
			data2[0] ^= 1;

			BlobKey key1a = put(server, jobId1, data, blobType);
			BlobKey key2 = put(server, jobId2, data, blobType);
			assertEquals(key1a, key2);

			BlobKey key1b = put(server, jobId1, data2, blobType);

			verifyContents(server, jobId1, key1a, data);
			verifyContents(server, jobId1, key1b, data2);
			checkFileCountForJob(2, jobId1, server);

			verifyContents(server, jobId2, key2, data);
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId1);

			verifyDeleted(server, jobId1, key1a);
			verifyDeleted(server, jobId1, key1b);
			checkFileCountForJob(0, jobId1, server);
			verifyContents(server, jobId2, key2, data);
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId2);

			checkFileCountForJob(0, jobId1, server);
			verifyDeleted(server, jobId2, key2);
			checkFileCountForJob(0, jobId2, server);

			// calling a second time should not fail
			server.cleanupJob(jobId2);
		}
	}

	@Test
	public void testConcurrentDeleteOperationsNoJobTransient()
			throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(null);
	}

	@Test
	public void testConcurrentDeleteOperationsForJobTransient()
			throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(new JobID());
=======
			// issue a DELETE request via the client
			deleteHelper(client, jobId, key);

			// issue a DELETE request on the server
			if (jobId == null) {
				server.delete(key);
			} else {
				server.delete(jobId, key);
			}

			// the file should still be there
			if (jobId == null) {
				server.getFile(key);
			} else {
				server.getFile(jobId, key);
			}
		} finally {
			if (blobFile != null && directory != null) {
				//noinspection ResultOfMethodCallIgnored
				blobFile.setWritable(true, false);
				//noinspection ResultOfMethodCallIgnored
				directory.setWritable(true, false);
			}
			cleanup(server, client);
		}
	}

	/**
	 * Tests that {@link BlobServer} cleans up after calling {@link BlobServer#cleanupJob(JobID)}.
	 */
	@Test
	public void testJobCleanup() throws IOException, InterruptedException {

		JobID jobId1 = new JobID();
		List<BlobKey> keys1 = new ArrayList<BlobKey>();
		JobID jobId2 = new JobID();
		List<BlobKey> keys2 = new ArrayList<BlobKey>();
		BlobServer server = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			BlobClient bc = new BlobClient(serverAddress, config);

			keys1.add(bc.put(jobId1, buf));
			keys2.add(bc.put(jobId2, buf));
			assertEquals(keys2.get(0), keys1.get(0));

			buf[0] += 1;
			keys1.add(bc.put(jobId1, buf));

			bc.close();

			assertEquals(2, checkFilesExist(jobId1, keys1, server, true));
			checkFileCountForJob(2, jobId1, server);
			assertEquals(1, checkFilesExist(jobId2, keys2, server, true));
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId1);

			checkFileCountForJob(0, jobId1, server);
			assertEquals(1, checkFilesExist(jobId2, keys2, server, true));
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId2);

			checkFileCountForJob(0, jobId1, server);
			checkFileCountForJob(0, jobId2, server);

			// calling a second time should not fail
			server.cleanupJob(jobId2);
		}
		finally {
			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent delete operations don't interfere with each other.
	 *
	 * Note: The test checks that there cannot be two threads which have checked whether a given blob file exist
	 * and then one of them fails deleting it. Without the introduced lock, this situation should rarely happen
	 * and make this test fail. Thus, if this test should become "unstable", then the delete atomicity is most likely
	 * broken.
	 */
	@Test
	public void testConcurrentDeleteOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(null);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	/**
	 * [FLINK-6020] Tests that concurrent delete operations don't interfere with each other.
	 *
	 * <p>Note: This test checks that there cannot be two threads which have checked whether a given
	 * blob file exist and then one of them fails deleting it. Without the introduced lock, this
	 * situation should rarely happen and make this test fail. Thus, if this test should become
	 * "unstable", then the delete atomicity is most likely broken.
	 *
	 * @param jobId
	 * 		job ID to use (or <tt>null</tt> if job-unrelated)
	 */
<<<<<<< HEAD
	private void testConcurrentDeleteOperations(@Nullable final JobID jobId)
			throws IOException, InterruptedException, ExecutionException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
=======
	@Test
	public void testConcurrentDeleteOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(new JobID());
	}

	private void testConcurrentDeleteOperations(final JobID jobId)
		throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		final BlobStore blobStore = mock(BlobStore.class);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		final int concurrentDeleteOperations = 3;
		final ExecutorService executor = Executors.newFixedThreadPool(concurrentDeleteOperations);

		final List<CompletableFuture<Void>> deleteFutures = new ArrayList<>(concurrentDeleteOperations);

		final byte[] data = {1, 2, 3};

<<<<<<< HEAD
		try (final BlobServer server = new BlobServer(config, new VoidBlobStore())) {
=======
		try (final BlobServer blobServer = new BlobServer(config, blobStore)) {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

			server.start();

<<<<<<< HEAD
			final TransientBlobKey blobKey =
				(TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);

			assertTrue(server.getStorageLocation(jobId, blobKey).exists());
=======
			try (BlobClient client = blobServer.createClient()) {
				blobKey = client.put(jobId, data);
			}

			assertTrue(blobServer.getStorageLocation(jobId, blobKey).exists());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

			for (int i = 0; i < concurrentDeleteOperations; i++) {
				CompletableFuture<Void> deleteFuture = CompletableFuture.supplyAsync(
					() -> {
<<<<<<< HEAD
						try {
							assertTrue(delete(server, jobId, blobKey));
							assertFalse(server.getStorageLocation(jobId, blobKey).exists());
							return null;
						} catch (IOException e) {
							throw new CompletionException(new FlinkException(
								"Could not delete the given blob key " + blobKey + '.'));
						}
					},
				executor);
=======
						try (BlobClient blobClient = blobServer.createClient()) {
							deleteHelper(blobClient, jobId, blobKey);
						} catch (IOException e) {
							throw new CompletionException(new FlinkException("Could not delete the given blob key " + blobKey + '.', e));
						}

						return null;
					},
					executor);
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

				deleteFutures.add(deleteFuture);
			}

			CompletableFuture<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

			// make sure all delete operation have completed successfully
			// in case of no lock, one of the delete operations should eventually fail
			waitFuture.get();

<<<<<<< HEAD
			assertFalse(server.getStorageLocation(jobId, blobKey).exists());

=======
			assertFalse(blobServer.getStorageLocation(jobId, blobKey).exists());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		} finally {
			executor.shutdownNow();
		}
	}

<<<<<<< HEAD
	/**
	 * Deletes a transient BLOB from the given BLOB service.
	 *
	 * @param service
	 * 		blob service
	 * @param jobId
	 * 		job ID (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key
	 *
	 * @return delete success
	 */
	static boolean delete(BlobService service, @Nullable JobID jobId, TransientBlobKey key) {
		if (jobId == null) {
			return service.getTransientBlobService().deleteFromCache(key);
		} else {
			return service.getTransientBlobService().deleteFromCache(jobId, key);
=======
	private static void cleanup(BlobServer server, BlobClient client) {
		if (client != null) {
			try {
				client.close();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		if (server != null) {
			try {
				server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		}
	}
}
