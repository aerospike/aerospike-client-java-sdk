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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            () -> qb.orderBy(null, OrderByType.INTEGER, Order.ASC));
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
    }

    @Test
    void orderByRejectsEmptyBinName() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("", OrderByType.INTEGER, Order.ASC));
    }

    @Test
    void orderByRejectsBinNameOverLengthLimit() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        String tooLong = "a".repeat(Bin.MAX_BIN_NAME_LENGTH + 1);
        assertThrows(AerospikeException.class, () -> qb.orderBy(tooLong, OrderByType.INTEGER, Order.ASC));
    }

    @Test
    void orderByRejectsNullType() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("n", null, Order.ASC));
    }

    @Test
    void orderByRejectsNullDirection() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class, () -> qb.orderBy("n", OrderByType.INTEGER, null));
    }

    @Test
    void orderByRejectsCaseInsensitiveFlagOnNonStringType() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class,
            () -> qb.orderBy("n", OrderByType.INTEGER, Order.ASC, OrderByFlags.CASE_INSENSITIVE));
    }

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

    @Test
    void thenOrderByRejectsMissingFirstKeyDuplicateAndThirdKey() {
        QueryBuilder empty = new QueryBuilder(session, dataSet());
        assertThrows(AerospikeException.class,
            () -> empty.thenOrderBy("second", OrderByType.INTEGER, Order.ASC));

        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("first", OrderByType.INTEGER, Order.ASC);
        assertThrows(AerospikeException.class,
            () -> qb.thenOrderBy("first", OrderByType.INTEGER, Order.DESC));
        qb.thenOrderBy("second", OrderByType.INTEGER, Order.DESC);
        assertThrows(AerospikeException.class,
            () -> qb.thenOrderBy("third", OrderByType.INTEGER, Order.ASC));
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
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC);

        AerospikeException ae = assertThrows(AerospikeException.class, qb::validateTopKQueryState);
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
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
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void pairingCheckIsOrderIndependent() {
        // Configure topK before orderBy.
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.topK(5).orderBy("n", OrderByType.INTEGER, Order.ASC);

        qb.validateTopKQueryState();
    }

    // -- incompatibility with limit/chunkSize/withNoBins -------------------------

    @Test
    void topKIsIncompatibleWithLimit() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5).limit(10);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void topKIsIncompatibleWithChunkSize() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5).chunkSize(100);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void topKIsIncompatibleWithWithNoBins() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5).withNoBins();

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    // -- projection membership ----------------------------------------------------

    @Test
    void orderByBinMustBeInProjectionWhenBinListProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5).readingOnlyBins("other");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInProjectionPassesWhenBinListProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5).readingOnlyBins("n", "other");

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinNotRequiredToBeInProjectionWhenNoProjectionIsSet() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinInProjectionPassesWhenSatisfiedOnlyBySelectFromOperation() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("similarity").selectFrom(Exp.intBin("n"));
        qb.orderBy("similarity", OrderByType.DOUBLE, Order.DESC).topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void orderByBinCannotBeProducedByMultipleOperations() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("n").get();
        qb.bin("n").get();
        qb.orderBy("n", OrderByType.INTEGER, Order.ASC).topK(1);

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInProjectionFailsWhenNotProducedByEitherProjectionMechanism() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        qb.bin("d").selectFrom(Exp.intBin("n"));
        qb.orderBy("similarity", OrderByType.DOUBLE, Order.DESC).topK(5).readingOnlyBins("stock");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void orderByBinInBinListButNotInOpsProjectionFailsBecauseOpsWinsOnWire() {
        QueryBuilder qb = new QueryBuilder(session, dataSet());
        // Operation projections replace bin-list projections, so they must produce the order key.
        qb.bin("similarity").selectFrom(Exp.intBin("n"));
        qb.orderBy("stock", OrderByType.INTEGER, Order.ASC).topK(5).readingOnlyBins("stock");

        assertThrows(AerospikeException.class, qb::validateTopKQueryState);
    }

    @Test
    void indexQueryInterfaceExposesOrderByThenOrderByAndTopK() {
        QueryBuilder qb = session.query(dataSet())
            .orderBy("n", OrderByType.INTEGER, Order.ASC)
            .thenOrderBy("m", OrderByType.STRING, Order.DESC)
            .topK(5);

        qb.validateTopKQueryState();
    }

    @Test
    void hybridVectorTopKQueryShapePassesClientSideValidation() {
        Vector queryVector = Vector.ofFloat32(new float[] {0.10f, 0.95f, 0.40f, 0.08f});

        QueryBuilder qb = new QueryBuilder(session, dataSet())
            .where(Exp.and(
                Exp.eq(Exp.stringBin("category"), Exp.val("electronics")),
                Exp.gt(Exp.intBin("stock"), Exp.val(0))));
        qb.bin("similarity").selectFrom(
            VectorExp.distance(VectorDistanceMetric.COSINE, queryVector, Exp.vectorBin("embedding")));
        qb.readingOnlyBins("name", "stock", "similarity")
            .orderBy("similarity", OrderByType.DOUBLE, Order.DESC)
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
            VectorExp.distance(VectorDistanceMetric.COSINE, queryVector, Exp.vectorBin("embedding")));
        qb.readingOnlyBins("name", "stock", "similarity")
            .orderBy("similarity", OrderByType.DOUBLE, Order.DESC)
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
            .orderBy("n", OrderByType.INTEGER, Order.ASC)
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
            .orderBy("n", OrderByType.INTEGER, Order.ASC)
            .topK(5);

        AerospikeException ae = assertThrows(AerospikeException.class,
            () -> qb.executeAsync(ErrorStrategy.IN_STREAM));
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
    }
}
