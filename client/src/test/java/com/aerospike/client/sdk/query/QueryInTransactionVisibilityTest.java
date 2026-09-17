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

import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.createIndexQuietly;
import static com.aerospike.client.sdk.query.QuerySelectionIntegSupport.dropIndexQuietly;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;

/**
 * What an application sees when it mixes a secondary-index query into a transaction.
 *
 * <p>A query carries no transaction: the server has no multi-record transaction support on the query path, so
 * the client does not forward one. The consequence is not an error but a disagreement. Within a single
 * transaction the same record reads as two different values depending on how it is asked for — a point read
 * participates and sees the transaction's own write, while a query runs outside and still sees the
 * pre-transaction state.</p>
 *
 * <pre>
 * session.doInTransaction(tx -&gt; {
 *     tx.upsert(order42).bin("status").setTo("shipped").execute();
 *
 *     tx.query(orders).where("$.status == 'shipped'")  // 0 rows, order42 missing
 *     tx.query(orders).where("$.status == 'pending'")  // 1 row, order42 still pending
 * });
 * </pre>
 *
 * <p>This is a server limitation rather than something the client can fix, so the test asserts it as the
 * contract. What the client owes the caller is an honest signal, which is what changed: an explicitly
 * supplied transaction is now refused at execute, and an inherited one logs a warning.
 * {@link QueryBuilder#notInAnyTransaction()} records that the non-participation is intended and silences it.
 * Before that, this behaviour was completely silent — no exception, no log, and a successful commit.</p>
 *
 * <p>Also worth knowing, and not observable here: rows a query returns never enter the transaction's read
 * set, so commit cannot detect that another writer changed them.</p>
 *
 * <p>The reads are stale rather than dirty. The query returns correctly committed data; it behaves as though
 * it ran immediately before the transaction opened.</p>
 *
 * <p>A write is kept in the transaction throughout, so the namespace is set and commit succeeds. A
 * transaction whose only activity is a query fails at commit for an unrelated reason (CLIENT-5404).</p>
 */
public class QueryInTransactionVisibilityTest extends ClusterTest {
    private static final String SET_NAME = "qtxnvis";
    private static final String INDEX_NAME = "qtxnvis_idx";
    private static final String BIN = "qvis";

    private static final int COMMITTED_VALUE = 1;
    private static final int UNCOMMITTED_VALUE = 2;

    private static DataSet dataSet;

    @BeforeAll
    static void prepare() {
        assumeTrue(args.scMode, "transactions require strong consistency");

        dataSet = DataSet.of(args.namespace, SET_NAME);
        createIndexQuietly(session, dataSet, INDEX_NAME, BIN, IndexType.INTEGER,
            IndexCollectionType.DEFAULT);
    }

    @AfterAll
    static void destroy() {
        if (dataSet == null) {
            return;
        }
        session.delete(dataSet.id("k1")).withDurableDelete().execute();
        dropIndexQuietly(session, dataSet, INDEX_NAME);
    }

    @Test
    void indexedQueryInsideTransactionReadsPreTransactionState() {
        Key key = dataSet.id("k1");
        seed(key);

        session.doInTransaction(txnSession -> {
            txnSession.upsert(key).bin(BIN).setTo(UNCOMMITTED_VALUE).execute();

            assertAll(
                // The point read participates, so it sees the write above.
                () -> assertEquals(UNCOMMITTED_VALUE, pointRead(txnSession, key),
                    "a point read inside the transaction should see the transaction's own write"),
                // The query does not, so it reads the record as it was before the transaction opened.
                () -> assertEquals(0, countMatching(txnSession, UNCOMMITTED_VALUE),
                    "a query must not match the value written inside the transaction"),
                () -> assertEquals(1, countMatching(txnSession, COMMITTED_VALUE),
                    "a query still matches the value the transaction overwrote"));
        });

        // Once committed the disagreement disappears, which is what made it easy to miss.
        assertAll(
            () -> assertEquals(UNCOMMITTED_VALUE, pointRead(session, key)),
            () -> assertEquals(1, countMatching(session, UNCOMMITTED_VALUE)),
            () -> assertEquals(0, countMatching(session, COMMITTED_VALUE)));
    }

    /**
     * {@code notInAnyTransaction()} is how a caller records that running outside the transaction is
     * deliberate. It silences the warning and must not change what the query returns, since the query was
     * never going to participate either way.
     */
    @Test
    void notInAnyTransactionIsAcceptedAndChangesNothing() {
        Key key = dataSet.id("k1");
        seed(key);

        session.doInTransaction(txnSession -> {
            txnSession.upsert(key).bin(BIN).setTo(UNCOMMITTED_VALUE).execute();

            assertAll(
                () -> assertEquals(0, countMatchingOutsideTransaction(txnSession, UNCOMMITTED_VALUE)),
                () -> assertEquals(1, countMatchingOutsideTransaction(txnSession, COMMITTED_VALUE)));
        });
    }

    private static void seed(Key key) {
        session.delete(key).withDurableDelete().execute();
        session.upsert(key).bin(BIN).setTo(COMMITTED_VALUE).execute();
    }

    private static Integer pointRead(Session session, Key key) {
        try (RecordStream rs = session.query(key).execute()) {
            return rs.hasNext() ? rs.next().recordOrThrow().getInt(BIN) : null;
        }
    }

    private static int countMatching(Session session, int value) {
        return drain(session.query(dataSet).where("$." + BIN + " == " + value).execute());
    }

    private static int countMatchingOutsideTransaction(Session session, int value) {
        return drain(session.query(dataSet)
            .where("$." + BIN + " == " + value)
            .notInAnyTransaction()
            .execute());
    }

    private static int drain(RecordStream rs) {
        try (rs) {
            int count = 0;
            while (rs.hasNext()) {
                rs.next().recordOrThrow();
                count++;
            }
            return count;
        }
    }
}
