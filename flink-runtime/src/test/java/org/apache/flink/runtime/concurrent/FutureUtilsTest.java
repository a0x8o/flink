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

package org.apache.flink.runtime.concurrent;

<<<<<<< HEAD
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;

=======
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

<<<<<<< HEAD
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
=======
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for the utility methods in {@link FutureUtils}.
 */
<<<<<<< HEAD
@RunWith(Parameterized.class)
public class FutureUtilsTest extends TestLogger{

	@Parameterized.Parameters
	public static Collection<FutureFactory> parameters (){
		return Arrays.asList(new ConjunctFutureFactory(), new WaitingFutureFactory());
	}

	@Parameterized.Parameter
	public FutureFactory futureFactory;

	@Test
	public void testConjunctFutureFailsOnEmptyAndNull() throws Exception {
		try {
			futureFactory.createFuture(null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			futureFactory.createFuture(Arrays.asList(
					new CompletableFuture<>(),
					null,
					new CompletableFuture<>()));
			fail();
		} catch (NullPointerException ignored) {}
	}
=======
public class FutureUtilsTest extends TestLogger {
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

	/**
	 * Tests that we can retry an operation.
	 */
	@Test
<<<<<<< HEAD
	public void testConjunctFutureCompletion() throws Exception {
		// some futures that we combine
		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// some future is initially completed
		future2.complete(new Object());

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(1, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete two more futures
		future4.complete(new Object());
		assertEquals(2, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete one future again
		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete the final future
		future3.complete(new Object());
		assertEquals(4, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());
	}

	@Test
	public void testConjunctFutureFailureOnFirst() throws Exception {

		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future2.completeExceptionally(new IOException());

		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());
=======
	public void testRetrySuccess() throws Exception {
		final int retries = 10;
		final AtomicInteger atomicInteger = new AtomicInteger(0);
		CompletableFuture<Boolean> retryFuture = FutureUtils.retry(
			() ->
				CompletableFuture.supplyAsync(
					() -> {
						if (atomicInteger.incrementAndGet() == retries) {
							return true;
						} else {
							throw new CompletionException(new FlinkException("Test exception"));
						}
					},
					TestingUtils.defaultExecutor()),
			retries,
			TestingUtils.defaultExecutor());

		assertTrue(retryFuture.get());
		assertTrue(retries == atomicInteger.get());
	}

	/**
	 * Tests that a retry future is failed after all retries have been consumed.
	 */
	@Test(expected = FutureUtils.RetryException.class)
	public void testRetryFailure() throws Throwable {
		final int retries = 3;
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		CompletableFuture<?> retryFuture = FutureUtils.retry(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			retries,
			TestingUtils.defaultExecutor());

		try {
			retryFuture.get();
		} catch (ExecutionException ee) {
			throw ExceptionUtils.stripExecutionException(ee);
		}
	}

	/**
	 * Tests that we can cancel a retry future.
	 */
	@Test
<<<<<<< HEAD
	public void testConjunctFutureFailureOnSuccessive() throws Exception {

		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));
		assertEquals(4, result.getNumFuturesTotal());

		java.util.concurrent.CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		future1.complete(new Object());
		future3.complete(new Object());
		future4.complete(new Object());

		future2.completeExceptionally(new IOException());

		assertEquals(3, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());

		try {
			result.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}

		try {
			resultMapped.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
=======
	public void testRetryCancellation() throws Exception {
		final int retries = 10;
		final AtomicInteger atomicInteger = new AtomicInteger(0);
		final OneShotLatch notificationLatch = new OneShotLatch();
		final OneShotLatch waitLatch = new OneShotLatch();
		final AtomicReference<Throwable> atomicThrowable = new AtomicReference<>(null);

		CompletableFuture<?> retryFuture = FutureUtils.retry(
			() ->
				CompletableFuture.supplyAsync(
					() -> {
						if (atomicInteger.incrementAndGet() == 2) {
							notificationLatch.trigger();
							try {
								waitLatch.await();
							} catch (InterruptedException e) {
								atomicThrowable.compareAndSet(null, e);
							}
						}

						throw new CompletionException(new FlinkException("Test exception"));
					},
					TestingUtils.defaultExecutor()),
			retries,
			TestingUtils.defaultExecutor());

		// await that we have failed once
		notificationLatch.await();

		assertFalse(retryFuture.isDone());

		// cancel the retry future
		retryFuture.cancel(false);

		// let the retry operation continue
		waitLatch.trigger();

		assertTrue(retryFuture.isCancelled());
		assertEquals(2, atomicInteger.get());

		if (atomicThrowable.get() != null) {
			throw new FlinkException("Exception occurred in the retry operation.", atomicThrowable.get());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
		}
	}

	/**
	 * Tests that retry with delay fails after having exceeded all retries.
	 */
<<<<<<< HEAD
	@Test
	public void testConjunctFutureValue() throws ExecutionException, InterruptedException {
		java.util.concurrent.CompletableFuture<Integer> future1 = java.util.concurrent.CompletableFuture.completedFuture(1);
		java.util.concurrent.CompletableFuture<Long> future2 = java.util.concurrent.CompletableFuture.completedFuture(2L);
		java.util.concurrent.CompletableFuture<Double> future3 = new java.util.concurrent.CompletableFuture<>();

		ConjunctFuture<Collection<Number>> result = FutureUtils.combineAll(Arrays.asList(future1, future2, future3));

		assertFalse(result.isDone());

		future3.complete(.1);

		assertTrue(result.isDone());
=======
	@Test(expected = FutureUtils.RetryException.class)
	public void testRetryWithDelayFailure() throws Throwable {
		CompletableFuture<?> retryFuture = FutureUtils.retryWithDelay(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			3,
			Time.milliseconds(1L),
			TestingUtils.defaultScheduledExecutor());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d

		try {
			retryFuture.get(TestingUtils.TIMEOUT().toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException ee) {
			throw ExceptionUtils.stripExecutionException(ee);
		}
	}

	/**
	 * Tests that the delay is respected between subsequent retries of a retry future with retry delay.
	 */
	@Test
<<<<<<< HEAD
	public void testConjunctOfNone() throws Exception {
		final ConjunctFuture<?> result = futureFactory.createFuture(Collections.<java.util.concurrent.CompletableFuture<Object>>emptyList());

		assertEquals(0, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
=======
	public void testRetryWithDelay() throws Exception {
		final int retries = 4;
		final Time delay = Time.milliseconds(50L);
		final AtomicInteger countDown = new AtomicInteger(retries);

		CompletableFuture<Boolean> retryFuture = FutureUtils.retryWithDelay(
			() -> {
				if (countDown.getAndDecrement() == 0) {
					return CompletableFuture.completedFuture(true);
				} else {
					return FutureUtils.completedExceptionally(new FlinkException("Test exception."));
				}
			},
			retries,
			delay,
			TestingUtils.defaultScheduledExecutor());

		long start = System.currentTimeMillis();

		Boolean result = retryFuture.get();

		long completionTime = System.currentTimeMillis() - start;

		assertTrue(result);
		assertTrue("The completion time should be at least rertries times delay between retries.", completionTime >= retries * delay.toMilliseconds());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}

	/**
	 * Tests that all scheduled tasks are canceled if the retry future is being cancelled.
	 */
<<<<<<< HEAD
	private interface FutureFactory {
		ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures);
	}

	private static class ConjunctFutureFactory implements FutureFactory {

		@Override
		public ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
			return FutureUtils.combineAll(futures);
		}
	}

	private static class WaitingFutureFactory implements FutureFactory {

		@Override
		public ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
			return FutureUtils.waitForAll(futures);
		}
=======
	@Test
	public void testRetryWithDelayCancellation() {
		ScheduledFuture<?> scheduledFutureMock = mock(ScheduledFuture.class);
		ScheduledExecutor scheduledExecutorMock = mock(ScheduledExecutor.class);
		doReturn(scheduledFutureMock).when(scheduledExecutorMock).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
		doAnswer(
			(InvocationOnMock invocation) -> {
				invocation.getArgumentAt(0, Runnable.class).run();
				return null;
			}).when(scheduledExecutorMock).execute(any(Runnable.class));

		CompletableFuture<?> retryFuture = FutureUtils.retryWithDelay(
			() -> FutureUtils.completedExceptionally(new FlinkException("Test exception")),
			1,
			TestingUtils.infiniteTime(),
			scheduledExecutorMock);

		assertFalse(retryFuture.isDone());

		verify(scheduledExecutorMock).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

		retryFuture.cancel(false);

		assertTrue(retryFuture.isCancelled());
		verify(scheduledFutureMock).cancel(anyBoolean());
>>>>>>> ebaa7b5725a273a7f8726663dbdf235c58ff761d
	}
}
