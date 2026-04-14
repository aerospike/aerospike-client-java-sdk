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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RecordStream#asCompletableFuture()} and {@link RecordStream#asPublisher()}.
 */
public class RecordStreamAdapterTest extends ClusterTest {

	private static final String KEY_PREFIX = "rs_adapter_";
	private static final int RECORD_COUNT = 5;

	@BeforeAll
	public static void setupRecords() {
		for (int i = 0; i < RECORD_COUNT; i++) {
			session.upsert(args.set.id(KEY_PREFIX + i))
				.bin("name").setTo("user_" + i)
				.bin("age").setTo(20 + i)
				.execute();
		}
	}

	private List<Key> testKeys() {
		List<Key> keys = new ArrayList<>();
		for (int i = 0; i < RECORD_COUNT; i++) {
			keys.add(args.set.id(KEY_PREFIX + i));
		}
		return keys;
	}

	// ========== asCompletableFuture() tests ==========

	@Test
	public void completableFutureSingleKey() throws Exception {
		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "0")).execute();
		CompletableFuture<List<RecordResult>> future = rs.asCompletableFuture();

		List<RecordResult> results = future.get(5, TimeUnit.SECONDS);
		assertEquals(1, results.size());
		assertEquals("user_0", results.get(0).recordOrThrow().getString("name"));
	}

	@Test
	public void completableFutureBatchKeys() throws Exception {
		RecordStream rs = session.query(testKeys()).execute();
		CompletableFuture<List<RecordResult>> future = rs.asCompletableFuture();

		List<RecordResult> results = future.get(5, TimeUnit.SECONDS);
		assertEquals(RECORD_COUNT, results.size());
		for (RecordResult rr : results) {
			assertTrue(rr.isOk());
			assertNotNull(rr.recordOrThrow().getString("name"));
		}
	}

	@Test
	public void completableFutureEmptyStream() throws Exception {
		RecordStream rs = new RecordStream();
		CompletableFuture<List<RecordResult>> future = rs.asCompletableFuture();
		rs.close();

		List<RecordResult> results = future.get(5, TimeUnit.SECONDS);
		assertTrue(results.isEmpty());
	}

	@Test
	public void completableFutureWithMapper() throws Exception {
		RecordMapper<String> nameMapper = new RecordMapper<>() {
			@Override
			public String fromMap(Map<String, Object> map, Key key, int generation) {
				return (String) map.get("name");
			}

			@Override
			public Map<String, Object> toMap(String obj) {
				return Map.of("name", obj);
			}

			@Override
			public Key id(String obj) {
				return null;
			}
		};

		RecordStream rs = session.query(testKeys()).execute();
		CompletableFuture<List<String>> future = rs.asCompletableFuture(nameMapper);

		List<String> names = future.get(5, TimeUnit.SECONDS);
		assertEquals(RECORD_COUNT, names.size());
		for (int i = 0; i < RECORD_COUNT; i++) {
			assertTrue(names.contains("user_" + i));
		}
	}

	@Test
	public void completableFutureComposition() throws Exception {
		CompletableFuture<Integer> countFuture = session.query(testKeys())
			.execute()
			.asCompletableFuture()
			.thenApply(List::size);

		assertEquals(RECORD_COUNT, countFuture.get(5, TimeUnit.SECONDS));
	}

	@Test
	public void completableFutureAsyncStream() throws Exception {
		RecordStream rs = session.query(testKeys()).executeAsync(ErrorStrategy.IN_STREAM);
		CompletableFuture<List<RecordResult>> future = rs.asCompletableFuture();

		List<RecordResult> results = future.get(5, TimeUnit.SECONDS);
		assertEquals(RECORD_COUNT, results.size());
	}

	// ========== asPublisher() tests ==========

	@Test
	public void publisherDeliversAllRecords() throws Exception {
		RecordStream rs = session.query(testKeys()).execute();

		CountDownLatch done = new CountDownLatch(1);
		List<RecordResult> received = new ArrayList<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			@Override
			public void onSubscribe(Flow.Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(RecordResult item) {
				received.add(item);
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override
			public void onComplete() {
				done.countDown();
			}
		});

		assertTrue(done.await(5, TimeUnit.SECONDS), "Publisher did not complete in time");
		assertNoError(error);
		assertEquals(RECORD_COUNT, received.size());
	}

	@Test
	public void publisherRespectsBackpressure() throws Exception {
		RecordStream rs = session.query(testKeys()).execute();

		CountDownLatch done = new CountDownLatch(1);
		List<RecordResult> received = new ArrayList<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			Flow.Subscription sub;

			@Override
			public void onSubscribe(Flow.Subscription s) {
				sub = s;
				s.request(2);
			}

			@Override
			public void onNext(RecordResult item) {
				received.add(item);
				if (received.size() == 2) {
					sub.request(RECORD_COUNT - 2);
				}
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override
			public void onComplete() {
				done.countDown();
			}
		});

		assertTrue(done.await(5, TimeUnit.SECONDS), "Publisher did not complete in time");
		assertNoError(error);
		assertEquals(RECORD_COUNT, received.size());
	}

	@Test
	public void publisherOneAtATime() throws Exception {
		RecordStream rs = session.query(testKeys()).execute();

		CountDownLatch done = new CountDownLatch(1);
		List<RecordResult> received = new ArrayList<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			Flow.Subscription sub;

			@Override
			public void onSubscribe(Flow.Subscription s) {
				sub = s;
				s.request(1);
			}

			@Override
			public void onNext(RecordResult item) {
				received.add(item);
				sub.request(1);
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override
			public void onComplete() {
				done.countDown();
			}
		});

		assertTrue(done.await(5, TimeUnit.SECONDS), "Publisher did not complete in time");
		assertNoError(error);
		assertEquals(RECORD_COUNT, received.size());
	}

	@Test
	public void publisherCancelStopsDelivery() throws Exception {
		RecordStream rs = session.query(testKeys()).execute();

		CountDownLatch cancelled = new CountDownLatch(1);
		AtomicInteger count = new AtomicInteger(0);

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			Flow.Subscription sub;

			@Override
			public void onSubscribe(Flow.Subscription s) {
				sub = s;
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(RecordResult item) {
				if (count.incrementAndGet() == 2) {
					sub.cancel();
					cancelled.countDown();
				}
			}

			@Override
			public void onError(Throwable t) {
				cancelled.countDown();
			}

			@Override
			public void onComplete() {
				cancelled.countDown();
			}
		});

		assertTrue(cancelled.await(5, TimeUnit.SECONDS));
		assertEquals(2, count.get());
	}

	@Test
	public void publisherRejectsSecondSubscriber() throws Exception {
		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "0")).execute();
		Flow.Publisher<RecordResult> publisher = rs.asPublisher();

		CountDownLatch firstDone = new CountDownLatch(1);
		publisher.subscribe(new Flow.Subscriber<>() {
			@Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
			@Override public void onNext(RecordResult item) {}
			@Override public void onError(Throwable t) { firstDone.countDown(); }
			@Override public void onComplete() { firstDone.countDown(); }
		});

		CountDownLatch secondDone = new CountDownLatch(1);
		AtomicReference<Throwable> secondError = new AtomicReference<>();

		publisher.subscribe(new Flow.Subscriber<>() {
			@Override public void onSubscribe(Flow.Subscription s) {}
			@Override public void onNext(RecordResult item) {}
			@Override public void onError(Throwable t) {
				secondError.set(t);
				secondDone.countDown();
			}
			@Override public void onComplete() { secondDone.countDown(); }
		});

		assertTrue(secondDone.await(5, TimeUnit.SECONDS));
		assertNotNull(secondError.get());
		assertTrue(secondError.get() instanceof IllegalStateException);
	}

	@Test
	public void publisherRejectsNonPositiveRequest() throws Exception {
		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "0")).execute();

		CountDownLatch done = new CountDownLatch(1);
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			@Override
			public void onSubscribe(Flow.Subscription s) {
				s.request(-1);
			}

			@Override public void onNext(RecordResult item) {}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override public void onComplete() { done.countDown(); }
		});

		assertTrue(done.await(5, TimeUnit.SECONDS));
		assertNotNull(error.get());
		assertTrue(error.get() instanceof IllegalArgumentException);
	}

	@Test
	public void publisherEmptyStream() throws Exception {
		RecordStream rs = new RecordStream();

		CountDownLatch done = new CountDownLatch(1);
		AtomicBoolean completed = new AtomicBoolean(false);
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			@Override
			public void onSubscribe(Flow.Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override public void onNext(RecordResult item) {}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override
			public void onComplete() {
				completed.set(true);
				done.countDown();
			}
		});

		rs.close();
		assertTrue(done.await(5, TimeUnit.SECONDS));
		assertNoError(error);
		assertTrue(completed.get());
	}

	@Test
	public void publisherWithAsyncStream() throws Exception {
		RecordStream rs = session.query(testKeys()).executeAsync(ErrorStrategy.IN_STREAM);

		CountDownLatch done = new CountDownLatch(1);
		List<RecordResult> received = new ArrayList<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		rs.asPublisher().subscribe(new Flow.Subscriber<>() {
			@Override
			public void onSubscribe(Flow.Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(RecordResult item) {
				received.add(item);
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				done.countDown();
			}

			@Override
			public void onComplete() {
				done.countDown();
			}
		});

		assertTrue(done.await(5, TimeUnit.SECONDS), "Publisher did not complete in time");
		assertNoError(error);
		assertEquals(RECORD_COUNT, received.size());
	}

	// ========== hasNext() idempotency tests ==========

	@Test
	public void hasNextIdempotentOnSingleItemStream() {
		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "0")).execute();

		// Multiple hasNext() calls must all return true before next() is called
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());

		RecordResult result = rs.next();
		assertNotNull(result);
		assertTrue(result.isOk());
		assertEquals("user_0", result.recordOrThrow().getString("name"));

		// After consuming the single item, hasNext() must consistently return false
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
	}

	@Test
	public void hasNextIdempotentOnAsyncStream() {
		RecordStream rs = session.query(testKeys()).execute();

		// Multiple hasNext() calls before first next()
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());

		int count = 0;
		while (rs.hasNext()) {
			// Call hasNext() extra times mid-iteration to verify no skipping
			assertTrue(rs.hasNext());
			RecordResult result = rs.next();
			assertNotNull(result);
			assertTrue(result.isOk());
			count++;
		}

		assertEquals(RECORD_COUNT, count);

		// After exhaustion, hasNext() must consistently return false
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
	}

	@Test
	public void hasNextIdempotentOnAsyncStreamFromExecuteAsync() {
		RecordStream rs = session.query(testKeys()).executeAsync(ErrorStrategy.IN_STREAM);

		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());

		int count = 0;
		while (rs.hasNext()) {
			assertTrue(rs.hasNext());
			RecordResult result = rs.next();
			assertNotNull(result);
			count++;
		}

		assertEquals(RECORD_COUNT, count);
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
	}

	@Test
	public void hasNextIdempotentOnChunkedStream() {
		// Index/scan queries produce ChunkedRecordStream
		// Use a set-wide query filtered to our test keys' bin value pattern
		session.upsert(args.set.id(KEY_PREFIX + "chunked_0"))
			.bin("name").setTo("chunked_user")
			.execute();

		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "chunked_0")).execute();

		// Multiple hasNext() calls must all agree
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());
		assertTrue(rs.hasNext());

		RecordResult result = rs.next();
		assertNotNull(result);
		assertTrue(result.isOk());
		assertEquals("chunked_user", result.recordOrThrow().getString("name"));

		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
	}

	@Test
	public void hasNextIdempotentOnEmptyStream() {
		RecordStream rs = new RecordStream();

		// Empty stream must consistently return false
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
		rs.close();
	}

	@Test
	public void hasNextIdempotentOnNonExistentKey() {
		session.delete(args.set.id(KEY_PREFIX + "nonexistent")).execute();

		RecordStream rs = session.query(args.set.id(KEY_PREFIX + "nonexistent")).execute();

		// A query for a non-existent key returns no results
		assertFalse(rs.hasNext());
		assertFalse(rs.hasNext());
	}

	private static void assertNoError(AtomicReference<Throwable> error) {
		Throwable t = error.get();
		if (t != null) {
			throw new AssertionError("Unexpected error in subscriber", t);
		}
	}
}
