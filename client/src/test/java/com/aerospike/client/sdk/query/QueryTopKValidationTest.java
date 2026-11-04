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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Bin;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.VectorExp;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/** Tests Top-K builder validation. */
public class QueryTopKValidationTest extends ClusterTest {
    private static DataSet dataSet;

    private static DataSet dataSet() {
        if (dataSet == null) {
            dataSet = DataSet.of(args.namespace, "topkvalidation");
        }
        return dataSet;
    }

    // -- orderBy(...) per-call argument validation -----------------------------

    @Test
    void orderByRejectsNullBinName() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> qb.orderBy(null, BinDataType.INTEGER, Order.ASC));
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
    }

    @Test
    void orderByRejectsEmptyBinName() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("", BinDataType.INTEGER, Order.ASC));
    }

    @Test
    void orderByRejectsBinNameOverLengthLimit() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        String tooLong = "a".repeat(Bin.MAX_BIN_NAME_LENGTH + 1);
        assertThrows(AerospikeException.class, () -> qb.orderBy(tooLong, BinDataType.INTEGER, Order.ASC));
    }

    @Test
    void orderByRejectsNullType() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("n", null, Order.ASC));
    }

    @Test
    void orderByRejectsNullDirection() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("n", BinDataType.INTEGER, null));
    }

    @Test
    void orderByRejectsCaseInsensitiveFlagOnNonStringType() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class,
            () -> qb.orderBy("n", BinDataType.INTEGER, Order.ASC, OrderByFlags.CASE_INSENSITIVE));
    }

    /* TODO Implement when server supports string order.
    @Test
    void orderByRejectsUnknownFlags() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class,
            () -> qb.orderBy("s", OrderByType.STRING, Order.ASC, 0x80));
    }

    @Test
    void orderByAcceptsCaseInsensitiveFlagOnStringType() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("s", OrderByType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
        assertEquals("s", qb.getOrderBySpec().getBinName());
        assertEquals(OrderByFlags.CASE_INSENSITIVE, qb.getOrderBySpec().getFlags());
    }

    @Test
    void thenOrderByAddsASecondIndependentSpec() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("first", OrderByType.INTEGER, Order.ASC)
            .thenOrderBy("second", OrderByType.STRING, Order.DESC, OrderByFlags.CASE_INSENSITIVE)
            .topK(5);

        assertEquals(2, qb.getOrderBySpecs().size());
        assertEquals("second", qb.getOrderBySpecs().get(1).getBinName());
    }
    */

    @Test
    void thenOrderByRejectsMissingFirstKeyAndThirdKey() {
        QueryBuilder empty = new QueryBuilder(session, dataSet());
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> empty.thenOrderBy("second", BinDataType.INTEGER, Order.ASC));
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());

        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("first", BinDataType.INTEGER, Order.ASC)
            .thenOrderBy("second", BinDataType.INTEGER, Order.DESC);
        ae = assertThrows(AerospikeException.class,
            () -> qb.thenOrderBy("third", BinDataType.INTEGER, Order.ASC));
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
        assertEquals(2, qb.getOrderBySpecs().size(), "the rejected call leaves both keys in place");
    }

    @Test
    void thenOrderByAddsASecondKeyAfterTheFirst() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("score", BinDataType.DOUBLE, Order.DESC)
            .thenOrderBy("ts", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        assertEquals(2, qb.getOrderBySpecs().size());
        assertEquals("score", qb.getOrderBySpecs().get(0).getBinName());
        assertEquals(Order.DESC, qb.getOrderBySpecs().get(0).getDirection());
        assertEquals("ts", qb.getOrderBySpecs().get(1).getBinName());
        assertEquals(Order.ASC, qb.getOrderBySpecs().get(1).getDirection());
        qb.validateTopKQueryState();
    }

    @Test
    void thenOrderByAcceptsTheFirstKeysBin() {
        // The PRD does not require distinct bins. The second key then never decides anything.
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC)
            .thenOrderBy("n", BinDataType.INTEGER, Order.DESC)
            .topK(5);

        assertEquals(2, qb.getOrderBySpecs().size());
        qb.validateTopKQueryState();
    }

    @Test
    void repeatedOrderByReplacesBothKeysAndKeepsTopK() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("score", BinDataType.DOUBLE, Order.DESC)
            .thenOrderBy("ts", BinDataType.INTEGER, Order.ASC)
            .topK(7)
            .orderBy("price", BinDataType.INTEGER, Order.ASC);

        assertEquals(1, qb.getOrderBySpecs().size());
        assertEquals("price", qb.getOrderBySpec().getBinName());
        assertEquals(7, qb.getTopK());

        // With the second key cleared, thenOrderBy may add one again.
        qb.thenOrderBy("ts", BinDataType.INTEGER, Order.ASC);
        assertEquals(2, qb.getOrderBySpecs().size());
    }

    @Test
    void secondKeyMustBeInProjection() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC)
            .thenOrderBy("m", BinDataType.INTEGER, Order.ASC)
            .topK(5)
            .readingOnlyBins("n");

        AerospikeException ae = assertThrows(AerospikeException.class, qb::validateTopKQueryState);
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
        assertTrue(ae.getMessage().contains("'m'"), ae.getMessage());

        qb.readingOnlyBins("n", "m");
        qb.validateTopKQueryState();
    }

    @Test
    void missingTypeMessageListsOnlySupportedTypes() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> qb.orderBy("n", null, Order.ASC));

        assertTrue(ae.getMessage().contains("INTEGER, DOUBLE, or BYTES"), ae.getMessage());
        assertFalse(ae.getMessage().contains("STRING"), ae.getMessage());
    }

    // -- result code of checks made before sending ----------------------------

    @Test
    void clientErrorCodeIsMinusTwoUntilAgreedAcrossClients() {
        assertEquals(-2, QueryBuilder.TOP_K_CLIENT_ERROR);
        assertEquals(ResultCode.PARSE_ERROR, QueryBuilder.TOP_K_CLIENT_ERROR);
    }

    @Test
    void everyCheckBeforeSendingFailsWithTheClientErrorCode() {
        // Server rejections keep PARAMETER_ERROR (4); none of these reach the server.
        Map<String, Consumer<QueryBuilder>> rejected = Map.ofEntries(
            Map.entry("null bin name", qb -> qb.orderBy(null, BinDataType.INTEGER, Order.ASC)),
            Map.entry("empty bin name", qb -> qb.orderBy("", BinDataType.INTEGER, Order.ASC)),
            Map.entry("bin name too long", qb -> qb.orderBy(
                "a".repeat(Bin.MAX_BIN_NAME_LENGTH + 1), BinDataType.INTEGER, Order.ASC)),
            Map.entry("null type", qb -> qb.orderBy("n", null, Order.ASC)),
            Map.entry("null direction", qb -> qb.orderBy("n", BinDataType.INTEGER, null)),
            Map.entry("unknown flags", qb -> qb.orderBy("n", BinDataType.INTEGER, Order.ASC, 0x80)),
            Map.entry("case-insensitive flag", qb -> qb.orderBy("n", BinDataType.INTEGER, Order.ASC,
                OrderByFlags.CASE_INSENSITIVE)),
            Map.entry("thenOrderBy first", qb -> qb.thenOrderBy("n", BinDataType.INTEGER, Order.ASC)),
            Map.entry("third key", qb -> qb.orderBy("a", BinDataType.INTEGER, Order.ASC)
                .thenOrderBy("b", BinDataType.INTEGER, Order.ASC)
                .thenOrderBy("c", BinDataType.INTEGER, Order.ASC)),
            Map.entry("topK zero", qb -> qb.topK(0)),
            Map.entry("topK over max", qb -> qb.topK(1001)),
            Map.entry("orderBy without topK", qb -> {
                qb.orderBy("n", BinDataType.INTEGER, Order.ASC);
                qb.validateTopKQueryState();
            }),
            Map.entry("topK without orderBy", qb -> {
                qb.topK(5);
                qb.validateTopKQueryState();
            }),
            Map.entry("with limit", qb -> {
                qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).limit(10);
                qb.validateTopKQueryState();
            }),
            Map.entry("with chunkSize", qb -> {
                qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).chunkSize(100);
                qb.validateTopKQueryState();
            }),
            Map.entry("with withNoBins", qb -> {
                qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).withNoBins();
                qb.validateTopKQueryState();
            }),
            Map.entry("not in projection", qb -> {
                qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).readingOnlyBins("other");
                qb.validateTopKQueryState();
            }),
            Map.entry("async", qb -> qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5)
                .executeAsync(ErrorStrategy.IN_STREAM)));

        rejected.forEach((name, call) -> {
            QueryBuilder qb = new QueryBuilder(session, dataSet());
            AerospikeException ae = assertThrows(AerospikeException.class, () -> call.accept(qb), name);
            assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode(), name + ": " + ae.getMessage());
        });
    }

    // -- topK(...) per-call argument validation --------------------------------

    @Test
    void topKRejectsZero() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.topK(0));
    }

    @Test
    void topKRejectsNegative() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.topK(-1));
    }

    @Test
    void topKRejectsOverMax() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.topK(1001));
    }

    @Test
    void topKAcceptsBoundaryValues() {
        QueryBuilder qb1 = new QueryBuilder(session, dataSet());
        qb1.topK(1);
        assertEquals(1, qb1.getTopK());

        QueryBuilder qb2 = new QueryBuilder(session, dataSet());
        qb2.topK(1000);
        assertEquals(1000, qb2.getTopK());
    }

    // -- default state ------------------------------------------------------------

    @Test
    void orderBySpecAndTopKAreNullByDefault() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertNull(qb.getOrderBySpec());
        assertNull(qb.getTopK());
        // Neither field is set.
        qb.validateTopKQueryState();
    }

    // -- cross-field pairing: orderBy XOR topK is an error -----------------------

    @Test
    void orderByWithoutTopKFailsPairingCheck() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC);

        AerospikeException ae = assertThrows(AerospikeException.class, qb::validateTopKQueryState);
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
    }

    @Test
    void topKWithoutOrderByFailsPairingCheck() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.topK(5);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByAndTopKTogetherPassesPairingCheck() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void pairingCheckIsOrderIndependent() {
        // Configure topK before orderBy.
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.topK(5).orderBy("n", BinDataType.INTEGER, Order.ASC);

        qb.validateTopKQueryState();
    }

    // -- incompatibility with limit/chunkSize/withNoBins -------------------------

    @Test
    void topKIsIncompatibleWithLimit() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).limit(10);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void topKIsIncompatibleWithChunkSize() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).chunkSize(100);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void topKIsIncompatibleWithWithNoBins() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).withNoBins();

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    // -- projection membership ----------------------------------------------------

    @Test
    void orderByBinMustBeInProjectionWhenBinListProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).readingOnlyBins("other");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInProjectionPassesWhenBinListProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5).readingOnlyBins("n", "other");

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinNotRequiredToBeInProjectionWhenNoProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinInProjectionPassesWhenSatisfiedOnlyBySelectFromOperation() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("similarity").selectFrom(Exp.intBin("n"));
        qb.orderBy("similarity", BinDataType.DOUBLE, Order.DESC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinCannotBeProducedByMultipleOperations() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("n").get();
        qb.bin("n").get();
        qb.orderBy("n", BinDataType.INTEGER, Order.ASC).topK(1);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInProjectionFailsWhenNotProducedByEitherProjectionMechanism() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("d").selectFrom(Exp.intBin("n"));
        qb.orderBy("similarity", BinDataType.DOUBLE, Order.DESC).topK(5).readingOnlyBins("stock");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInBinListButNotInOpsProjectionFailsBecauseOpsWinsOnWire() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        // Operation projections replace bin-list projections, so they must produce the order key.
        qb.bin("similarity").selectFrom(Exp.intBin("n"));
        qb.orderBy("stock", BinDataType.INTEGER, Order.ASC).topK(5).readingOnlyBins("stock");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    /* TODO Implement when server supports string order.
    @Test
    void indexQueryInterfaceExposesOrderByThenOrderByAndTopK() {
        QueryBuilder qb = session.query(dataSet())
            .orderBy("n", OrderByType.INTEGER, Order.ASC)
            .thenOrderBy("m", OrderByType.STRING, Order.DESC)
            .topK(5);

        qb.validateTopKQueryState();
    }
    */

    @Test
    void hybridVectorTopKQueryShapePassesClientSideValidation() {
        Vector queryVector = Vector.ofFloat32(new float[] {0.10f, 0.95f, 0.40f, 0.08f});

        QueryBuilder qb = new QueryBuilder(session, dataSet())
            .where(Exp.and(
                Exp.eq(Exp.stringBin("category"), Exp.val("electronics")),
                Exp.gt(Exp.intBin("stock"), Exp.val(0))));
        qb.bin("similarity").selectFrom(
            VectorExp.distance(VectorDistanceMetric.COSINE_SIMILARITY, queryVector, Exp.vectorBin("embedding")));
        qb.readingOnlyBins("name", "stock", "similarity")
            .orderBy("similarity", BinDataType.DOUBLE, Order.DESC)
            .topK(10);

        qb.validateTopKQueryState();
    }

    @Test
    void hybridVectorTopKQueryShapeExecutesOnCapableServer() {
        Vector queryVector = Vector.ofFloat32(new float[] {0.10f, 0.95f, 0.40f, 0.08f});

        QueryBuilder qb = new QueryBuilder(session, dataSet())
            .where(Exp.and(
                Exp.eq(Exp.stringBin("category"), Exp.val("electronics")),
                Exp.gt(Exp.intBin("stock"), Exp.val(0))));
        qb.bin("similarity").selectFrom(
            VectorExp.distance(VectorDistanceMetric.COSINE_SIMILARITY, queryVector, Exp.vectorBin("embedding")));
        qb.readingOnlyBins("name", "stock", "similarity")
            .orderBy("similarity", BinDataType.DOUBLE, Order.DESC)
            .topK(10);

        try (var results = qb.execute()) {
            while (results.hasNext()) {
                results.next().recordOrThrow();
            }
        }
    }

    @Test
    void clusterAdvertisesTopKSupport() {
        assertTrue(cluster.supportsTopK());
    }

    @Test
    void executingATopKQueryAgainstThisClusterIsAccepted() {
        QueryBuilder qb = new QueryBuilder(session, dataSet())
            .orderBy("n", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        try (var results = qb.execute()) {
            while (results.hasNext()) {
                results.next().recordOrThrow();
            }
        }
    }

    @Test
    void asyncTopKIsRejectedBeforeQueryExecution() {
        QueryBuilder qb = new QueryBuilder(session, dataSet())
            .orderBy("n", BinDataType.INTEGER, Order.ASC)
            .topK(5);

        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> qb.executeAsync(ErrorStrategy.IN_STREAM));
        assertEquals(QueryBuilder.TOP_K_CLIENT_ERROR, ae.getResultCode());
    }
}
