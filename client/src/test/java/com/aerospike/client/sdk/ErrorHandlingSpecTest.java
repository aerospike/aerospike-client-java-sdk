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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.junit.ServerFeatureSupport;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.query.IndexType;
import com.aerospike.client.sdk.query.QueryBuilder;

/**
 * Asserts the error-channel spec discussed for CLIENT-5521, not today's implementation.
 *
 * <p>Some methods are expected to fail until the client matches this contract:</p>
 * <ol>
 *   <li><b>Programming errors</b> (null strategy/handler, illegal builder use) throw immediately
 *       from {@code execute} and {@code executeAsync}.</li>
 *   <li><b>Terminal failures</b> (whole operation cannot run: missing hard-hinted index, …).
 *       Sync {@code execute} throws regardless of {@link ErrorHandler} /
 *       {@link ErrorStrategy}. Async {@code executeAsync} returns a {@link RecordStream}; the
 *       failure is {@link AsyncRecordStream#error(Throwable)} — iteration throws,
 *       {@code asCompletableFuture} completes exceptionally. The handler is never invoked. The
 *       future must not complete with an error {@link RecordResult}.</li>
 *   <li><b>Per-key / per-row</b>. {@code IN_STREAM} and default batch {@code execute()} put each
 *       key in the stream. {@code ErrorHandler} overloads omit failed keys and call the handler.
 *       Single-key {@code execute()} throws for actionable errors. {@code KEY_NOT_FOUND} on a
 *       point read is omitted unless {@code includeMissingKeys()}; then it is a stream row
 *       (not a throw or handler call), including on single-key {@code execute()}.
 *       {@link ResultCode#INVALID_NAMESPACE} is per-key
 *       <em>outside</em> a transaction. Mixed namespaces <em>inside</em> a transaction
 *       (explicit or implicit MRT) fail the whole submitted operation client-side.</li>
 * </ol>
 */
public class ErrorHandlingSpecTest extends ClusterTest {

    private static final String SET = "errspec_" + Long.toHexString(System.nanoTime());
    private static final String INDEX = SET + "_age_idx";
    private static final String AGE = "age";
    private static final String VAL = "val";

    private static DataSet ds;
    private static Key presentA;
    private static Key presentB;
    private static Key missing;
    private static Key insertTarget;

    @BeforeAll
    static void seed() {
        ds = DataSet.of(args.namespace, SET);
        presentA = ds.id("a");
        presentB = ds.id("b");
        missing = ds.id("missing");
        insertTarget = ds.id("insert_me");

        session.delete(presentA, presentB, missing, insertTarget).execute();
        session.upsert(presentA).bin(VAL).setTo(1).bin(AGE).setTo(20).execute();
        session.upsert(presentB).bin(VAL).setTo(2).bin(AGE).setTo(30).execute();
        session.delete(missing).execute();
        session.delete(insertTarget).execute();

        try {
            session.createIndex(ds, INDEX, AGE, IndexType.INTEGER, IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }

    @AfterAll
    static void tearDown() {
        if (session == null) {
            return;
        }
        try {
            session.delete(presentA, presentB, missing, insertTarget).execute();
        }
        catch (RuntimeException ignored) {
        }
        try {
            session.dropIndex(ds, INDEX).waitTillComplete();
        }
        catch (RuntimeException ignored) {
        }
    }

    // ------------------------------------------------------------------ programming errors

    @Nested
    @DisplayName("1. Programming errors throw immediately (sync and async)")
    class ProgrammingErrors {

        static Stream<Arguments> nullStrategy() {
            return Stream.of(
                Arguments.of("index query execute",
                    (Executable) () -> session.query(ds).where("$." + AGE + " > 0")
                        .execute((ErrorStrategy) null)),
                Arguments.of("index query executeAsync",
                    (Executable) () -> session.query(ds).where("$." + AGE + " > 0")
                        .executeAsync((ErrorStrategy) null)),
                Arguments.of("point query execute",
                    (Executable) () -> session.query(presentA).execute((ErrorStrategy) null)),
                Arguments.of("point query executeAsync",
                    (Executable) () -> session.query(presentA).executeAsync((ErrorStrategy) null)),
                Arguments.of("batch query execute",
                    (Executable) () -> session.query(presentA, presentB).execute((ErrorStrategy) null)),
                Arguments.of("batch query executeAsync",
                    (Executable) () -> session.query(presentA, presentB).executeAsync((ErrorStrategy) null)),
                Arguments.of("point write execute",
                    (Executable) () -> session.upsert(presentA).bin(VAL).setTo(9)
                        .execute((ErrorStrategy) null)),
                Arguments.of("point write executeAsync",
                    (Executable) () -> session.upsert(presentA).bin(VAL).setTo(9)
                        .executeAsync((ErrorStrategy) null)),
                Arguments.of("batch write execute",
                    (Executable) () -> session.upsert(presentA, presentB).bin(VAL).setTo(9)
                        .execute((ErrorStrategy) null)),
                Arguments.of("batch write executeAsync",
                    (Executable) () -> session.upsert(presentA, presentB).bin(VAL).setTo(9)
                        .executeAsync((ErrorStrategy) null)));
        }

        static Stream<Arguments> nullHandler() {
            return Stream.of(
                Arguments.of("index query execute",
                    (Executable) () -> session.query(ds).where("$." + AGE + " > 0")
                        .execute((ErrorHandler) null)),
                Arguments.of("index query executeAsync",
                    (Executable) () -> session.query(ds).where("$." + AGE + " > 0")
                        .executeAsync((ErrorHandler) null)),
                Arguments.of("point query execute",
                    (Executable) () -> session.query(presentA).execute((ErrorHandler) null)),
                Arguments.of("point query executeAsync",
                    (Executable) () -> session.query(presentA).executeAsync((ErrorHandler) null)),
                Arguments.of("batch query execute",
                    (Executable) () -> session.query(presentA, presentB).execute((ErrorHandler) null)),
                Arguments.of("batch query executeAsync",
                    (Executable) () -> session.query(presentA, presentB).executeAsync((ErrorHandler) null)),
                Arguments.of("point write execute",
                    (Executable) () -> session.upsert(presentA).bin(VAL).setTo(9)
                        .execute((ErrorHandler) null)),
                Arguments.of("point write executeAsync",
                    (Executable) () -> session.upsert(presentA).bin(VAL).setTo(9)
                        .executeAsync((ErrorHandler) null)),
                Arguments.of("batch write execute",
                    (Executable) () -> session.upsert(presentA, presentB).bin(VAL).setTo(9)
                        .execute((ErrorHandler) null)),
                Arguments.of("batch write executeAsync",
                    (Executable) () -> session.upsert(presentA, presentB).bin(VAL).setTo(9)
                        .executeAsync((ErrorHandler) null)));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("nullStrategy")
        void nullErrorStrategyThrowsImmediately(String unused, Executable call) {
            assertThrows(NullPointerException.class, call);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("nullHandler")
        void nullErrorHandlerThrowsImmediately(String unused, Executable call) {
            assertThrows(NullPointerException.class, call);
        }

        @Test
        void illegalPartitionRangeThrowsBeforeExecute() {
            QueryBuilder qb = new QueryBuilder(session, ds);
            assertThrows(IllegalArgumentException.class, () -> qb.onPartitionRange(-1, 1));
        }
    }

    // ------------------------------------------------------------------ terminal failures

    @Nested
    @DisplayName("2. Terminal failures: sync throws; async fails the stream/CF; handler never runs")
    class TerminalFailures {

        @Nested
        @DisplayName("Index query: hard hint on a missing index")
        class MissingIndexHint {

            @BeforeEach
            void requireQuerySelection() {
                ServerFeatureSupport.assume(ServerFeature.QUERY_SELECTION);
            }

            private QueryBuilder failing() {
                return session.query(ds)
                    .where("$." + AGE + " > 10")
                    .withHint(h -> h.forIndex("doesnt_exist").hardHint());
            }

            @Test
            void executeThrows() {
                CountingHandler h = new CountingHandler();
                AerospikeException ae = assertThrows(AerospikeException.class, () -> failing().execute());
                assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
                assertFalse(h.called());
            }

            @Test
            void executeInStreamThrows() {
                CountingHandler h = new CountingHandler();
                AerospikeException ae = assertThrows(AerospikeException.class,
                    () -> failing().execute(ErrorStrategy.IN_STREAM));
                assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
                assertFalse(h.called());
            }

            @Test
            void executeHandlerThrowsAndDoesNotInvokeHandler() {
                CountingHandler h = new CountingHandler();
                AerospikeException ae = assertThrows(AerospikeException.class, () -> failing().execute(h));
                assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
                assertFalse(h.called(), "ErrorHandler is per-record; planning failure must not invoke it");
            }

            @Test
            void executeAsyncInStreamFailsCompletableFuture() {
                CountingHandler h = new CountingHandler();
                assertAsyncTerminal(() -> failing().executeAsync(ErrorStrategy.IN_STREAM), h);
            }

            @Test
            void executeAsyncHandlerFailsCompletableFutureAndDoesNotInvokeHandler() {
                CountingHandler h = new CountingHandler();
                assertAsyncTerminal(() -> failing().executeAsync(h), h);
            }
        }
    }

    // ------------------------------------------------------------------ per-key / per-row

    @Nested
    @DisplayName("3. Per-key / per-row (not terminal)")
    class PerKey {

        @Nested
        @DisplayName("Point read KEY_NOT_FOUND")
        class PointReadMissing {

            @Test
            void executeWithoutIncludeMissingKeysIsEmptyNotThrown() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(session.query(missing).execute());
                assertTrue(rows.isEmpty());
                assertFalse(h.called());
            }

            @Test
            void executeIncludeMissingKeysPutsKeyNotFoundInStream() {
                List<RecordResult> rows = collect(session.query(missing).includeMissingKeys().execute());
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, rows.get(0).getResultCode());
            }

            @Test
            void executeInStreamIncludeMissingKeysPutsKeyNotFoundInStream() {
                List<RecordResult> rows = collect(
                    session.query(missing).includeMissingKeys().execute(ErrorStrategy.IN_STREAM));
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, rows.get(0).getResultCode());
            }

            @Test
            void executeHandlerLeavesKeyNotFoundInStream() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(
                    session.query(missing).includeMissingKeys().execute(h));
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, rows.get(0).getResultCode());
                assertFalse(h.called(), "KEY_NOT_FOUND on a read is informational, not an ErrorHandler event");
            }

            @Test
            void executeAsyncInStreamCompletesWithKeyNotFoundRow() {
                List<RecordResult> rows = session.query(missing)
                    .includeMissingKeys()
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, rows.get(0).getResultCode());
            }
        }

        @Nested
        @DisplayName("Point write KEY_EXISTS (insert)")
        class PointInsertExists {

            @Test
            void executeThrows() {
                CountingHandler h = new CountingHandler();
                AerospikeException ae = assertThrows(AerospikeException.class,
                    () -> session.insert(presentA).bin(VAL).setTo(99).execute());
                assertEquals(ResultCode.KEY_EXISTS_ERROR, ae.getResultCode());
                assertFalse(h.called());
            }

            @Test
            void executeInStreamPutsErrorInStream() {
                List<RecordResult> rows = collect(
                    session.insert(presentA).bin(VAL).setTo(99).execute(ErrorStrategy.IN_STREAM));
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_EXISTS_ERROR, rows.get(0).getResultCode());
            }

            @Test
            void executeHandlerInvokesHandlerAndOmitsRow() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(session.insert(presentA).bin(VAL).setTo(99).execute(h));
                assertTrue(rows.isEmpty());
                assertEquals(1, h.count.get());
                assertEquals(ResultCode.KEY_EXISTS_ERROR, h.lastCode.get());
            }

            @Test
            void executeAsyncInStreamCompletesWithErrorRow() {
                List<RecordResult> rows = session.insert(presentA).bin(VAL).setTo(99)
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(1, rows.size());
                assertEquals(ResultCode.KEY_EXISTS_ERROR, rows.get(0).getResultCode());
            }

            @Test
            void executeAsyncHandlerInvokesHandlerAndOmitsRow() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = session.insert(presentA).bin(VAL).setTo(99)
                    .executeAsync(h)
                    .asCompletableFuture()
                    .join();
                assertTrue(rows.isEmpty());
                assertEquals(1, h.count.get());
            }
        }

        @Nested
        @DisplayName("INVALID_NAMESPACE is per-key outside a transaction")
        class InvalidNamespace {

            private Key mistyped() {
                return DataSet.of("no_such_namespace_errspec", "s").id("mistyped");
            }

            @Test
            void batchWriteMixedNamespacesPutsEachKeyInStream() {
                List<RecordResult> rows = collect(
                    session.upsert(presentA, mistyped()).bin(VAL).setTo(1)
                        .notInAnyTransaction()
                        .execute());
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.INVALID_NAMESPACE));
            }

            @Test
            void batchWriteMixedNamespacesExecuteAsyncCompletesWithBothRows() {
                List<RecordResult> rows = session.upsert(presentA, mistyped()).bin(VAL).setTo(1)
                    .notInAnyTransaction()
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.INVALID_NAMESPACE));
            }

            @Test
            void batchWriteMixedNamespacesHandlerOmitsInvalidKey() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(
                    session.upsert(presentA, mistyped()).bin(VAL).setTo(1)
                        .notInAnyTransaction()
                        .execute(h));
                assertEquals(1, rows.size());
                assertTrue(rows.get(0).isOk());
                assertEquals(1, h.count.get());
                assertEquals(ResultCode.INVALID_NAMESPACE, h.lastCode.get());
            }

            @Test
            void batchWriteMixedNamespacesInTransactionThrows() {
                assumeTrue(args.scMode, "transactions require strong consistency");
                CountingHandler h = new CountingHandler();
                session.doInTransaction(txnSession -> {
                    AerospikeException ae = assertThrows(AerospikeException.class,
                        () -> txnSession.upsert(presentA, mistyped()).bin(VAL).setTo(1).execute());
                    assertTrue(ae.getMessage().contains("Namespace must be the same"),
                        ae.getMessage());
                    assertFalse(h.called());
                });
            }

            @Test
            void batchWriteMixedNamespacesInTransactionAsyncFailsCompletableFuture() {
                assumeTrue(args.scMode, "transactions require strong consistency");
                CountingHandler h = new CountingHandler();
                session.doInTransaction(txnSession -> {
                    assertAsyncTerminal(
                        () -> txnSession.upsert(presentA, mistyped()).bin(VAL).setTo(1)
                            .executeAsync(ErrorStrategy.IN_STREAM),
                        h);
                });
            }

            @Test
            void implicitMrtMixedNamespacesThrows() {
                assumeTrue(args.scMode, "implicit MRT applies to CP batch writes");
                CountingHandler h = new CountingHandler();
                AerospikeException ae = assertThrows(AerospikeException.class,
                    () -> session.upsert(presentA, mistyped()).bin(VAL).setTo(1).execute());
                assertTrue(ae.getMessage().contains("Namespace must be the same"),
                    ae.getMessage());
                assertFalse(h.called());
            }

            @Test
            void implicitMrtMixedNamespacesAsyncFailsCompletableFuture() {
                assumeTrue(args.scMode, "implicit MRT applies to CP batch writes");
                CountingHandler h = new CountingHandler();
                assertAsyncTerminal(
                    () -> session.upsert(presentA, mistyped()).bin(VAL).setTo(1)
                        .executeAsync(ErrorStrategy.IN_STREAM),
                    h);
            }

            @Test
            void batchQueryMixedNamespacesPutsEachKeyInStream() {
                List<RecordResult> rows = collect(session.query(presentA, mistyped()).execute());
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.INVALID_NAMESPACE));
            }

            @Test
            void pointQueryExecuteThrows() {
                AerospikeException ae = assertThrows(AerospikeException.class,
                    () -> session.query(mistyped()).execute());
                assertEquals(ResultCode.INVALID_NAMESPACE, ae.getResultCode());
            }

            @Test
            void pointQueryExecuteInStreamPutsErrorInStream() {
                List<RecordResult> rows = collect(
                    session.query(mistyped()).execute(ErrorStrategy.IN_STREAM));
                assertEquals(1, rows.size());
                assertEquals(ResultCode.INVALID_NAMESPACE, rows.get(0).getResultCode());
            }

            @Test
            void pointQueryExecuteAsyncInStreamCompletesWithErrorRow() {
                List<RecordResult> rows = session.query(mistyped())
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(1, rows.size());
                assertEquals(ResultCode.INVALID_NAMESPACE, rows.get(0).getResultCode());
            }

            @Test
            void pointWriteExecuteThrows() {
                AerospikeException ae = assertThrows(AerospikeException.class,
                    () -> session.upsert(mistyped()).bin(VAL).setTo(1).execute());
                assertEquals(ResultCode.INVALID_NAMESPACE, ae.getResultCode());
            }

            @Test
            void pointWriteExecuteAsyncInStreamCompletesWithErrorRow() {
                List<RecordResult> rows = session.upsert(mistyped()).bin(VAL).setTo(1)
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(1, rows.size());
                assertEquals(ResultCode.INVALID_NAMESPACE, rows.get(0).getResultCode());
            }
        }

        @Nested
        @DisplayName("Batch read: mix of present and missing")
        class BatchRead {

            @Test
            void executePutsEachKeyInStream() {
                List<RecordResult> rows = collect(
                    session.query(presentA, missing).includeMissingKeys().execute());
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.KEY_NOT_FOUND_ERROR));
            }

            @Test
            void executeHandlerLeavesMissingKeyInStream() {
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(
                    session.query(presentA, missing).includeMissingKeys().execute(h));
                assertEquals(2, rows.size());
                assertFalse(h.called(), "KEY_NOT_FOUND on a read is not an ErrorHandler event");
            }

            @Test
            void executeAsyncInStreamCompletesWithBothRows() {
                List<RecordResult> rows = session.query(presentA, missing)
                    .includeMissingKeys()
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.KEY_NOT_FOUND_ERROR));
            }
        }

        @Nested
        @DisplayName("Batch write: mix of KEY_EXISTS and success")
        class BatchInsertMixed {

            @Test
            void defaultExecutePutsEachKeyInStream() {
                session.delete(insertTarget).execute();
                List<RecordResult> rows = collect(
                    session.insert(presentA, insertTarget).bin(VAL).setTo(7).execute());
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.KEY_EXISTS_ERROR));
            }

            @Test
            void executeInStreamPutsEachKeyInStream() {
                session.delete(insertTarget).execute();
                List<RecordResult> rows = collect(
                    session.insert(presentA, insertTarget).bin(VAL).setTo(7)
                        .execute(ErrorStrategy.IN_STREAM));
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.KEY_EXISTS_ERROR));
            }

            @Test
            void executeHandlerOmitsFailedKey() {
                session.delete(insertTarget).execute();
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(
                    session.insert(presentA, insertTarget).bin(VAL).setTo(7).execute(h));
                assertEquals(1, rows.size());
                assertTrue(rows.get(0).isOk());
                assertEquals(1, h.count.get());
                assertEquals(ResultCode.KEY_EXISTS_ERROR, h.lastCode.get());
            }

            @Test
            void executeAsyncInStreamCompletesNormallyWithBothRows() {
                session.delete(insertTarget).execute();
                List<RecordResult> rows = session.insert(presentA, insertTarget).bin(VAL).setTo(7)
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertEquals(2, rows.size());
                assertEquals(1, countCode(rows, ResultCode.OK));
                assertEquals(1, countCode(rows, ResultCode.KEY_EXISTS_ERROR));
            }

            @Test
            void executeAsyncHandlerOmitsFailedKey() {
                session.delete(insertTarget).execute();
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = session.insert(presentA, insertTarget).bin(VAL).setTo(7)
                    .executeAsync(h)
                    .asCompletableFuture()
                    .join();
                assertEquals(1, rows.size());
                assertTrue(rows.get(0).isOk());
                assertEquals(1, h.count.get());
            }
        }

        @Nested
        @DisplayName("Index query happy path still returns rows")
        class IndexQueryOk {

            @Test
            void executeHandlerDoesNotFireOnSuccess() {
                ServerFeatureSupport.assume(ServerFeature.QUERY_SELECTION);
                CountingHandler h = new CountingHandler();
                List<RecordResult> rows = collect(
                    session.query(ds).where("$." + AGE + " > 10").execute(h));
                assertFalse(rows.isEmpty());
                assertTrue(rows.stream().allMatch(RecordResult::isOk));
                assertFalse(h.called());
            }

            @Test
            void executeAsyncInStreamCompletesWithRows() {
                ServerFeatureSupport.assume(ServerFeature.QUERY_SELECTION);
                List<RecordResult> rows = session.query(ds).where("$." + AGE + " > 10")
                    .executeAsync(ErrorStrategy.IN_STREAM)
                    .asCompletableFuture()
                    .join();
                assertFalse(rows.isEmpty());
                assertTrue(rows.stream().allMatch(RecordResult::isOk));
            }
        }
    }

    // ------------------------------------------------------------------ spec helpers

    /**
     * {@code executeAsync} must return; the future must fail with {@link AerospikeException};
     * completing with a list (including a single error {@link RecordResult}) is wrong.
     */
    private static void assertAsyncTerminal(Supplier<RecordStream> start, CountingHandler handler) {
        RecordStream stream = assertDoesNotThrow(start::get,
            "executeAsync must return a RecordStream; terminal failure is delivered on the stream");
        try {
            List<RecordResult> rows = stream.asCompletableFuture().join();
            fail("terminal async failure must completeExceptionally, not succeed with " + summarize(rows));
        }
        catch (CompletionException ce) {
            Throwable cause = unwrap(ce);
            assertInstanceOf(AerospikeException.class, cause,
                "CF cause should be AerospikeException, was " + cause);
            assertFalse(handler.called(),
                "ErrorHandler is per-record; terminal failure must not invoke it");
        }
    }

    private static Throwable unwrap(Throwable t) {
        Throwable cur = t;
        while (cur instanceof CompletionException && cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static List<RecordResult> collect(RecordStream stream) {
        try (stream) {
            List<RecordResult> rows = new ArrayList<>();
            while (stream.hasNext()) {
                rows.add(stream.next());
            }
            return rows;
        }
    }

    private static long countCode(List<RecordResult> rows, int code) {
        return rows.stream().filter(r -> r.getResultCode() == code).count();
    }

    private static String summarize(List<RecordResult> rows) {
        return rows.stream()
            .map(r -> Integer.toString(r.getResultCode()))
            .toList()
            .toString();
    }

    private static final class CountingHandler implements ErrorHandler {
        final AtomicInteger count = new AtomicInteger();
        final AtomicInteger lastCode = new AtomicInteger(ResultCode.OK);
        final AtomicBoolean invoked = new AtomicBoolean();

        @Override
        public void handle(Key key, int index, AerospikeException exception) {
            invoked.set(true);
            count.incrementAndGet();
            lastCode.set(exception.getResultCode());
        }

        boolean called() {
            return invoked.get();
        }
    }
}
