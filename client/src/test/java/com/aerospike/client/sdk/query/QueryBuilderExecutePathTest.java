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
package com.aerospike.client.sdk.query;

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.AGE_BIN;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.RECORD_COUNT;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.assumeQuerySelection;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.collectAges;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.destroyQselint;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.prepareQselint;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorHandler;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.KnownDefect;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.query.QuerySelectionIntegSupport.Fixture;

/**
 * Dataset {@link QueryBuilder} / {@link IndexQueryBuilderImpl} execute overload coverage.
 *
 * <p>Documents known gaps: {@code execute(ErrorStrategy)} and {@code executeAsync(ErrorStrategy)}
 * null-check the strategy then call {@code executeInternal(null)}; {@code executeAsync} is not
 * actually async on the dataset path; {@link QueryBuilder#getTxnToUse()} is never read by
 * {@link IndexQueryBuilderImpl}.</p>
 *
 * <p>{@link #chunkedExecuteWithErrorHandlerPaginatesEntireSet()} is expected to fail: on a chunked
 * query, {@code execute(ErrorHandler)} truncates the result set to the first chunk.</p>
 */
public class QueryBuilderExecutePathTest extends ClusterTest {
    private static final Fixture FIXTURE = Fixture.forSuffix("qbexec");
    private static final String BOGUS_INDEX = "qbexec_missing_idx";
    private static final String AGE_RANGE_WHERE = "$." + AGE_BIN + " >= 14 and $." + AGE_BIN + " <= 18";
    private static final List<Integer> AGE_RANGE_ROWS = List.of(14, 15, 16, 17, 18);
    private static final String ALL_AGES_WHERE =
        "$." + AGE_BIN + " >= 1 and $." + AGE_BIN + " <= " + RECORD_COUNT;
    private static final int CHUNK_SIZE = 5;

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeQuerySelection();
        dataSet = prepareQselint(session, args.namespace, FIXTURE);
    }

    @AfterAll
    static void destroy() {
        destroyQselint(session, dataSet, FIXTURE);
    }

    // ---------------------------------------------------------------- null guards

    @Test
    void executeNullStrategyThrows() {
        QueryBuilder qb = okQuery();
        assertThrows(NullPointerException.class, () -> qb.execute((ErrorStrategy) null));
    }

    @Test
    void executeNullHandlerThrows() {
        QueryBuilder qb = okQuery();
        assertThrows(NullPointerException.class, () -> qb.execute((ErrorHandler) null));
    }

    @Test
    void executeAsyncNullStrategyThrows() {
        QueryBuilder qb = okQuery();
        assertThrows(NullPointerException.class, () -> qb.executeAsync((ErrorStrategy) null));
    }

    @Test
    void executeAsyncNullHandlerThrows() {
        QueryBuilder qb = okQuery();
        assertThrows(NullPointerException.class, () -> qb.executeAsync((ErrorHandler) null));
    }

    // ---------------------------------------------------------------- error paths (hard hint)

    @Test
    void executeHardHintThrowsIndexNotFound() {
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> failingQuery().execute());

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
    }

    /**
     * {@link IndexQueryBuilderImpl#execute(ErrorStrategy)} discards the strategy and still throws
     * at plan time for an unrecoverable hint violation.
     */
    @Test
    void executeInStreamStrategyStillThrowsOnHardHint() {
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> failingQuery().execute(ErrorStrategy.IN_STREAM));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
    }

    @Test
    void executeAsyncInStreamStrategyStillThrowsOnHardHint() {
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> failingQuery().executeAsync(ErrorStrategy.IN_STREAM));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
    }

    @Test
    void executeErrorHandlerStillThrowsOnHardHint() {
        AtomicInteger handled = new AtomicInteger();
        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            failingQuery().execute((key, index, ex) -> handled.incrementAndGet()));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
        assertEquals(0, handled.get(), "handler must not run when planning fails");
    }

    @Test
    void executeAsyncErrorHandlerStillThrowsOnHardHint() {
        AtomicInteger handled = new AtomicInteger();
        AerospikeException ae = assertThrows(AerospikeException.class, () ->
            failingQuery().executeAsync((key, index, ex) -> handled.incrementAndGet()));

        assertEquals(ResultCode.INDEX_NOTFOUND, ae.getResultCode());
        assertEquals(0, handled.get(), "handler must not run when planning fails");
    }

    // ---------------------------------------------------------------- happy-path handler + async

    @Test
    void executeErrorHandlerReturnsSuccessfulRows() {
        AtomicInteger handled = new AtomicInteger();
        List<Integer> ages = collectAges(
            okQuery().execute((key, index, ex) -> handled.incrementAndGet()),
            AGE_BIN);

        assertEquals(AGE_RANGE_ROWS, ages);
        assertEquals(0, handled.get());
    }

    @Test
    void executeAsyncErrorHandlerReturnsSuccessfulRows() {
        AtomicInteger handled = new AtomicInteger();
        try (RecordStream rs = okQuery().executeAsync((key, index, ex) -> handled.incrementAndGet())) {
            List<Integer> ages = collectAges(rs, AGE_BIN);
            assertEquals(AGE_RANGE_ROWS, ages);
        }
        assertEquals(0, handled.get());
    }

    @Test
    void executeAsyncInStreamReturnsSuccessfulRows() {
        try (RecordStream rs = okQuery().executeAsync(ErrorStrategy.IN_STREAM)) {
            assertEquals(AGE_RANGE_ROWS, collectAges(rs, AGE_BIN));
        }
    }

    /**
     * {@link QueryBuilder#warnIfInTransaction()} runs when {@code executeAsync} is called with an
     * active transaction on the session (no explicit {@code inTransaction} call required).
     */
    @Test
    @Tag(KnownDefect.TAG)
    @Disabled("Known defect: committing a transaction whose only activity was a query fails. The server has"
        + " no MRT support on the query path (as_transaction_has_mrt_id is never consulted under"
        + " as/src/query, and records are read with is_mrt hardcoded false), so the client correctly does"
        + " not forward the transaction. But that also means Txn.setNamespace is never called, and TxnRoll"
        + " then does partitionMap.get(null) and throws InvalidNamespace at commit.")
    void executeAsyncInsideTransactionCompletes() {
        assumeTrue(args.scMode, "transactions require strong consistency");

        session.doInTransaction(txnSession -> {
            TxnExposingBuilder qb = new TxnExposingBuilder(txnSession, dataSet);
            assertNotNull(qb.exposedTxnToUse(), "session transaction should bind to QueryBuilder");

            try (RecordStream rs = qb
                .readingOnlyBins(AGE_BIN)
                .where(AGE_RANGE_WHERE)
                .executeAsync(ErrorStrategy.IN_STREAM)) {
                assertEquals(AGE_RANGE_ROWS, collectAges(rs, AGE_BIN));
            }
        });
    }

    /**
     * {@link QueryBuilder#getTxnToUse()} is populated but {@link IndexQueryBuilderImpl} never
     * forwards it to the wire command.
     */
    @Test
    @Tag(KnownDefect.TAG)
    @Disabled("Known defect: this transaction runs no command at all, so Txn.namespace stays null and"
        + " TxnRoll throws InvalidNamespace at commit. Committing an empty transaction should be a no-op.")
    void explicitInTransactionSetsTxnOnBuilder() {
        assumeTrue(args.scMode, "transactions require strong consistency");

        session.doInTransaction(txnSession -> {
            Txn txn = txnSession.getCurrentTransaction();
            assertNotNull(txn);

            TxnExposingBuilder qb = new TxnExposingBuilder(txnSession, dataSet);
            qb.inTransaction(txn);

            assertEquals(txn, qb.exposedTxnToUse());
        });
    }

    // ---------------------------------------------------------------- chunked pagination

    /**
     * Control: with the no-arg overload, driving {@code hasMoreChunks()} / {@code hasNext()} over a
     * chunked query yields every matching row. Chunking itself is sound.
     */
    @Test
    void chunkedExecutePaginatesEntireSet() {
        ChunkDrain drain = drainAllChunks(chunkedQuery().execute());

        assertAll(
            () -> assertEquals(RECORD_COUNT, drain.records()),
            () -> assertTrue(drain.chunks() > 1,
                "the set must span multiple chunks for this test to mean anything, but the query "
                    + "completed in " + drain.chunks() + " chunk(s)"));
    }

    /**
     * Expected to fail. {@code IndexQueryBuilderImpl.execute(ErrorHandler)} wraps the stream in
     * {@code AbstractFilterableBuilder.filterStreamErrors}, which drains the source with
     * {@code forEach} — that only walks the current chunk — and returns a stream over the buffered
     * list. The {@code ChunkedRecordStream} is discarded, so the caller cannot reach the remaining
     * chunks and silently receives {@code chunkSize} rows instead of the full set.
     *
     * <p>Compare {@link #chunkedExecutePaginatesEntireSet()} (no handler, correct) and
     * {@link #chunkedExecuteAsyncWithErrorHandlerPaginatesEntireSet()} (handler installed on the
     * stream rather than post-filtered, correct). The only difference is the wrapper.</p>
     */
    @Test
    @Disabled("Pending fix")
    void chunkedExecuteWithErrorHandlerPaginatesEntireSet() {
        AtomicInteger handled = new AtomicInteger();
        ChunkDrain drain = drainAllChunks(
            chunkedQuery().execute((key, index, ex) -> handled.incrementAndGet()));

        assertAll(
            () -> assertEquals(0, handled.get(), "no row in the fixture fails"),
            () -> assertEquals(RECORD_COUNT, drain.records()));
    }

    /**
     * Contrast: {@code executeAsync(ErrorHandler)} installs the handler on the stream via
     * {@code executeInternal(handler)} instead of post-filtering it, so chunked pagination survives.
     */
    @Test
    void chunkedExecuteAsyncWithErrorHandlerPaginatesEntireSet() {
        AtomicInteger handled = new AtomicInteger();
        ChunkDrain drain = drainAllChunks(
            chunkedQuery().executeAsync((key, index, ex) -> handled.incrementAndGet()));

        assertAll(
            () -> assertEquals(0, handled.get(), "no row in the fixture fails"),
            () -> assertEquals(RECORD_COUNT, drain.records()));
    }

    // ---------------------------------------------------------------- transaction mode conflicts

    @Test
    void notInAnyTransactionTwiceThrows() {
        QueryBuilder qb = new QueryBuilder(session, dataSet);
        qb.notInAnyTransaction();

        AerospikeException ae = assertThrows(AerospikeException.class, qb::notInAnyTransaction);
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
    }

    @Test
    @Tag(KnownDefect.TAG)
    @Disabled("Known defect: the assertion under test passes, but the enclosing doInTransaction block runs"
        + " no command, so Txn.namespace stays null and TxnRoll throws InvalidNamespace at commit.")
    void notInAnyTransactionThenInTransactionThrows() {
        assumeTrue(args.scMode, "transactions require strong consistency");

        session.doInTransaction(txnSession -> {
            QueryBuilder qb = new QueryBuilder(txnSession, dataSet);
            qb.notInAnyTransaction();

            AerospikeException ae = assertThrows(AerospikeException.class,
                () -> qb.inTransaction(txnSession.getCurrentTransaction()));
            assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        });
    }

    private static QueryBuilder okQuery() {
        return session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(AGE_RANGE_WHERE);
    }

    private static QueryBuilder failingQuery() {
        return session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(AGE_RANGE_WHERE)
            .withHint(hint -> hint.forIndex(BOGUS_INDEX).hardHint());
    }

    /** Matches all {@value QuerySelectionIntegSupport#RECORD_COUNT} fixture rows, chunked. */
    private static QueryBuilder chunkedQuery() {
        return session.query(dataSet)
            .readingOnlyBins(AGE_BIN)
            .where(ALL_AGES_WHERE)
            .chunkSize(CHUNK_SIZE);
    }

    /** Rows and chunks observed while draining a stream with the documented chunked-query loop. */
    private record ChunkDrain(int records, int chunks) {}

    private static ChunkDrain drainAllChunks(RecordStream rs) {
        int records = 0;
        int chunks = 0;

        try (rs) {
            while (rs.hasMoreChunks()) {
                chunks++;
                while (rs.hasNext()) {
                    rs.next().recordOrThrow();
                    records++;
                }
            }
        }
        return new ChunkDrain(records, chunks);
    }

    /** Exposes {@link QueryBuilder#getTxnToUse()} for assertions in this package. */
    private static final class TxnExposingBuilder extends QueryBuilder {
        TxnExposingBuilder(Session session, DataSet dataSet) {
            super(session, dataSet);
        }

        Txn exposedTxnToUse() {
            return getTxnToUse();
        }
    }
}
