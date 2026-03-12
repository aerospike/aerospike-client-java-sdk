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
package com.aerospike.client.fluent.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.ClusterTest;
import com.aerospike.client.fluent.ErrorStrategy;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Session;

/**
 * Tests getting a record by key via {@code session.query(key).executeAsync(ErrorStrategy.IN_STREAM)}
 * and classifying the outcome as: record received, timeout exception, or other exception.
 * Pattern borrowed from {@link com.aerospike.benchmarks.RWTaskAsync}.
 */
public class QueryByKeyExecuteAsyncTest extends ClusterTest {

	private static final String KEY_PREFIX = "query_async_key_";
	private static final String BIN_NAME = "bin";

	@BeforeEach
	public void cleanupTestKeys() {
		session.delete(args.set.id(KEY_PREFIX + "exists")).execute();
		session.delete(args.set.id(KEY_PREFIX + "nonexistent")).execute();
	}

	@Test
	public void getRecordByQueryExecuteAsync_existingKey_returnsRecordReceived() throws Exception {
		Key key = args.set.id(KEY_PREFIX + "exists");
		session.upsert(key)
			.bin(BIN_NAME).setTo("value")
			.execute();

		QueryAsyncOutcome outcome = getRecordByQueryExecuteAsync(session, key);

		assertEquals(QueryAsyncOutcome.OutcomeType.RECORD_RECEIVED, outcome.type);
		assertNotNull(outcome.record);
		assertEquals("value", outcome.record.getString(BIN_NAME));
		assertNull(outcome.exception);
	}

	@Test
	public void getRecordByQueryExecuteAsync_nonexistentKey_returnsNotFoundOrEmpty() throws Exception {
		Key key = args.set.id(KEY_PREFIX + "nonexistent");

		QueryAsyncOutcome outcome = getRecordByQueryExecuteAsync(session, key);

		assertTrue(
			outcome.type == QueryAsyncOutcome.OutcomeType.NOT_FOUND_OR_EMPTY
				|| (outcome.type == QueryAsyncOutcome.OutcomeType.RECORD_RECEIVED && outcome.record == null),
			"Expected NOT_FOUND_OR_EMPTY or RECORD_RECEIVED with null record, got " + outcome.type
		);
		assertNull(outcome.exception);
	}

	@Test
	public void classifyException_timeout_returnsTimeoutException() {
		AerospikeException timeoutEx = AerospikeException.resultCodeToException(
			ResultCode.TIMEOUT, "timeout", false);

		QueryAsyncOutcome.OutcomeType type = classifyException(timeoutEx);

		assertEquals(QueryAsyncOutcome.OutcomeType.TIMEOUT_EXCEPTION, type);
	}

	@Test
	public void classifyException_queryTimeout_returnsTimeoutException() {
		AerospikeException queryTimeoutEx = AerospikeException.resultCodeToException(
			ResultCode.QUERY_TIMEOUT, "query timeout", false);

		QueryAsyncOutcome.OutcomeType type = classifyException(queryTimeoutEx);

		assertEquals(QueryAsyncOutcome.OutcomeType.TIMEOUT_EXCEPTION, type);
	}

	@Test
	public void classifyException_otherAerospikeException_returnsOtherException() {
		AerospikeException otherEx = AerospikeException.resultCodeToException(
			ResultCode.KEY_NOT_FOUND_ERROR, "not found", false);

		QueryAsyncOutcome.OutcomeType type = classifyException(otherEx);

		assertEquals(QueryAsyncOutcome.OutcomeType.OTHER_EXCEPTION, type);
	}

	@Test
	public void classifyException_runtimeException_returnsOtherException() {
		QueryAsyncOutcome.OutcomeType type = classifyException(new RuntimeException("fail"));

		assertEquals(QueryAsyncOutcome.OutcomeType.OTHER_EXCEPTION, type);
	}

	// --- Helpers (pattern from RWTaskAsync) ---

	/**
	 * Runs {@code session.query(key).executeAsync(ErrorStrategy.IN_STREAM)}, collects the
	 * result or exception, and classifies as RECORD_RECEIVED, NOT_FOUND_OR_EMPTY,
	 * TIMEOUT_EXCEPTION, or OTHER_EXCEPTION.
	 */
	public static QueryAsyncOutcome getRecordByQueryExecuteAsync(Session session, Key key)
		throws InterruptedException, ExecutionException, TimeoutException {
		RecordStream handle = session.query(key)
			.executeAsync(ErrorStrategy.IN_STREAM);

		CompletableFuture<QueryAsyncOutcome> outcomeFuture = handle
			.asCompletableFuture()
			.<QueryAsyncOutcome>handle((results, ex) -> {
				if (ex != null) {
					// Use ex as-is; CompletableFuture.handle() receives the throwable passed to completeExceptionally()
					QueryAsyncOutcome.OutcomeType type = classifyException(ex);
					return new QueryAsyncOutcome(type, null, ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
				}
				return classifyResults(results);
			})
			.whenComplete((r, ex) -> handle.close());

		return outcomeFuture.get(10, TimeUnit.SECONDS);
	}

	private static QueryAsyncOutcome classifyResults(List<RecordResult> results) {
		if (results == null || results.isEmpty()) {
			return new QueryAsyncOutcome(QueryAsyncOutcome.OutcomeType.NOT_FOUND_OR_EMPTY, null, null);
		}
		RecordResult first = results.get(0);
		if (first.isOk()) {
			Record record = first.recordOrNull();
			return new QueryAsyncOutcome(
				record != null ? QueryAsyncOutcome.OutcomeType.RECORD_RECEIVED : QueryAsyncOutcome.OutcomeType.NOT_FOUND_OR_EMPTY,
				record,
				null);
		}
		// In-stream error (e.g. timeout or other from server)
		if (first.resultCode() == ResultCode.TIMEOUT || first.resultCode() == ResultCode.QUERY_TIMEOUT) {
			return new QueryAsyncOutcome(
				QueryAsyncOutcome.OutcomeType.TIMEOUT_EXCEPTION,
				null,
				first.exception() != null ? first.exception() : AerospikeException.resultCodeToException(first.resultCode(), first.message(), first.inDoubt()));
		}
		return new QueryAsyncOutcome(
			QueryAsyncOutcome.OutcomeType.OTHER_EXCEPTION,
			null,
			first.exception() != null ? first.exception() : AerospikeException.resultCodeToException(first.resultCode(), first.message(), first.inDoubt()));
	}

	/**
	 * Classifies an exception from the async future as TIMEOUT_EXCEPTION or OTHER_EXCEPTION.
	 * Mirrors {@link com.aerospike.benchmarks.RWTask#readFailure(AerospikeException)} logic.
	 */
	public static QueryAsyncOutcome.OutcomeType classifyException(Throwable ex) {
		if (ex instanceof AerospikeException) {
			int code = ((AerospikeException) ex).getResultCode();
			if (code == ResultCode.TIMEOUT || code == ResultCode.QUERY_TIMEOUT) {
				return QueryAsyncOutcome.OutcomeType.TIMEOUT_EXCEPTION;
			}
		}
		return QueryAsyncOutcome.OutcomeType.OTHER_EXCEPTION;
	}

	public static final class QueryAsyncOutcome {
		public enum OutcomeType {
			RECORD_RECEIVED,
			NOT_FOUND_OR_EMPTY,
			TIMEOUT_EXCEPTION,
			OTHER_EXCEPTION
		}

		public final OutcomeType type;
		public final Record record;
		public final Exception exception;

		public QueryAsyncOutcome(OutcomeType type, Record record, Exception exception) {
			this.type = type;
			this.record = record;
			this.exception = exception;
		}
	}
}
