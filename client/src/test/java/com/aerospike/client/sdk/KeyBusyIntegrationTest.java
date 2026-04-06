/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * Best-effort integration test for {@link ResultCode#KEY_BUSY} (14): many threads hammer the same
 * key without client-side retries so the first hot-key rejection is not masked.
 * <p>
 * Whether the server returns KEY_BUSY depends on version, load, and configuration; the test
 * skips if 14 was not observed in the allotted time.
 * </p>
 */
public class KeyBusyIntegrationTest extends ClusterTest {

	private static final String BIN = "k";

	@Test
	public void concurrentWritesOnOneKey_mayObserveKeyBusy() throws Exception {
		Key key = args.set.id("keyBusyIntegration");
		session.upsert(key).bins(BIN).values(0).execute();

		// One call attempt per write so KEY_BUSY is not masked by client retries (see SyncExecutor).
		Behavior noRetryOnBusy = Behavior.DEFAULT.deriveWithChanges("keyBusyIntegration", b -> b
				.on(Selectors.writes().retryable(), ops -> ops.maximumNumberOfCallAttempts(1)));
		Session stressSession = cluster.createSession(noRetryOnBusy);

		final int threads = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);
		final CyclicBarrier barrier = new CyclicBarrier(threads + 1);
		final AtomicBoolean sawKeyBusy = new AtomicBoolean(false);
		final AtomicReference<AerospikeException> firstKeyBusy = new AtomicReference<>();
		final long runDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);

		try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
			for (int t = 0; t < threads; t++) {
				final int tid = t;
				es.submit(() -> {
					try {
						barrier.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					} catch (BrokenBarrierException e) {
						return;
					}
					int i = tid;
					while (!sawKeyBusy.get() && System.nanoTime() < runDeadlineNanos) {
						try {
							stressSession.upsert(key)
								.bins(BIN)
								.values(i++)
								.execute();
						} catch (AerospikeException e) {
							if (e.getResultCode() == ResultCode.KEY_BUSY) {
								firstKeyBusy.compareAndSet(null, e);
								sawKeyBusy.set(true);
								return;
							}
							throw e;
						}
					}
				});
			}

			barrier.await();
			es.shutdown();
			assumeTrue(es.awaitTermination(20, TimeUnit.SECONDS),
					"Worker threads did not finish in time");
		}

		assumeTrue(sawKeyBusy.get(),
				"Server did not return KEY_BUSY (14) in this run; try more threads, a slower machine, "
						+ "or heavier contention — hot-key is timing-dependent.");

		AerospikeException ae = firstKeyBusy.get();
		assertTrue(ae instanceof AerospikeException.KeyBusyException);
		assertEquals(ResultCode.KEY_BUSY, ae.getResultCode());
	}
}
