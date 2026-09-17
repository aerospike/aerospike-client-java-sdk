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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.ExpressionReadOptions;
import com.aerospike.client.sdk.HllConfig;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Value.HLLValue;
import com.aerospike.client.sdk.ael.Ael;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.util.Version;

/**
 * Integration tests for {@link QueryBuilderBinBuilder} — the dataset-query
 * ({@code session.query(dataSet).bin(...)}) read path.
 *
 * <p>Key-based query tests ({@code session.query(key).bin(...)}) exercise the parallel
 * {@link com.aerospike.client.sdk.QueryBinBuilder} class instead; this class closes the
 * JaCoCo gap on the dataset path.</p>
 */
public class QueryBuilderBinBuilderTest extends ClusterTest {

    private static final String INDEX = "qbbb_idx";
    private static final String FILTER = "qbbb_f";
    private static final String AGE = "qbbb_age";
    private static final String SCORES = "qbbb_scores";
    private static final String TAGS = "qbbb_tags";
    private static final String SETTINGS = "qbbb_settings";
    private static final String LONG_MAP = "qbbb_lmap";
    private static final String STR_MAP = "qbbb_smap";
    private static final String STR_VAL_MAP = "qbbb_svmap";
    private static final String BLOB_MAP = "qbbb_bmap";
    private static final String BLOB_VAL_MAP = "qbbb_bvmap";
    private static final String NESTED = "qbbb_nested";
    private static final String HLL_BIN = "qbbb_hll";
    private static final String HLL_OTHER = "qbbb_hll_other";
    private static final String BIT_BIN = "qbbb_bits";
    private static final String KEY_ID = "qbbb_seed";

    private static final int FILTER_VAL = 900_001;

    private static Key seedKey;

    @BeforeAll
    public static void prepare() {
        Assumptions.assumeTrue(
            session.getCluster().getVersion().isGreaterOrEqual(Version.SERVER_VERSION_8_1_2),
            "dataset bin projection requires server 8.1.2+");
        assumeSupportsAel();

        try {
            session.createIndex(args.set, INDEX, FILTER, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        seedKey = args.set.id(KEY_ID);
        session.delete(seedKey).execute();

        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("theme", "dark");
        settings.put("volume", 10);
        settings.put("notifications", true);
        settings.put("ratio", 1.5);

        Map<Long, Long> longMap = new LinkedHashMap<>();
        longMap.put(10L, 100L);
        longMap.put(20L, 200L);
        longMap.put(30L, 300L);

        Map<String, Long> strMap = new LinkedHashMap<>();
        strMap.put("alpha", 10L);
        strMap.put("beta", 20L);
        strMap.put("gamma", 30L);

        Map<String, String> strValMap = new LinkedHashMap<>();
        strValMap.put("k1", "beta");
        strValMap.put("k2", "gamma");
        strValMap.put("k3", "omega");

        byte[] blobKey = new byte[] {0x01, 0x02};
        Map<byte[], Long> blobMap = new HashMap<>();
        blobMap.put(blobKey, 42L);

        byte[] blobValue = new byte[] {0x0A, 0x0B};
        Map<byte[], byte[]> blobValueMap = new HashMap<>();
        blobValueMap.put(new byte[] {0x07}, blobValue);

        Map<String, Object> nested = Map.of(
            "users", List.of("Alice", "Bob", "Charlie"),
            "counts", List.of(10, 20, 30));

        List<String> hllEntries = List.of("hll-a", "hll-b", "hll-c");
        List<String> hllOtherEntries = List.of("hll-b", "hll-c", "hll-d");

        byte[] tagBlob = "python".getBytes();

        session.upsert(seedKey)
            .bin(FILTER).setTo(FILTER_VAL)
            .bin(AGE).setTo(25)
            .bin(SCORES).setTo(List.of(10, 20, 30, 40, 50))
            .bin(TAGS).setTo(List.of("java", tagBlob, "rust"))
            .bin(SETTINGS).setTo(settings)
            .bin(LONG_MAP).setTo(longMap)
            .bin(STR_MAP).setTo(strMap)
            .bin(STR_VAL_MAP).setTo(strValMap)
            .bin(BLOB_MAP).setTo(blobMap)
            .bin(BLOB_VAL_MAP).setTo(blobValueMap)
            .bin(NESTED).setTo(nested)
            .bin(BIT_BIN).setTo(new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0xF0, (byte) 0x0F})
            .bin(HLL_BIN).hllAdd(hllEntries, HllConfig.of(8))
            .bin(HLL_OTHER).hllAdd(hllOtherEntries, HllConfig.of(8))
            .execute();
    }

    @AfterAll
    public static void destroy() {
        session.dropIndex(args.set, INDEX);
    }

    private static QueryBuilder filteredQuery() {
        return session.query(args.set)
            .where("$." + FILTER + " == " + FILTER_VAL);
    }

    private static Record executeOne(QueryBuilder builder) {
        try (RecordStream rs = builder.execute()) {
            assertTrue(rs.hasNext());
            return rs.next().recordOrThrow();
        }
    }

    @Nested
    class TopLevelCdt {

        @Test
        void listAndMapSizeAndGet() {
            assertEquals(5L, executeOne(filteredQuery().bin(SCORES).listSize()).getLong(SCORES));
            assertArrayEquals(tagBytes(),
                executeOne(filteredQuery().bin(TAGS).listGet(1)).getBytes(TAGS));

            List<?> twoTags = executeOne(filteredQuery().bin(TAGS).listGetRange(0, 2)).getList(TAGS);
            assertEquals(2, twoTags.size());
            assertEquals("java", twoTags.get(0));
            assertArrayEquals(tagBytes(), (byte[]) twoTags.get(1));

            List<?> lastTag = executeOne(filteredQuery().bin(TAGS).listGetRange(2)).getList(TAGS);
            assertEquals(List.of("rust"), lastTag);

            assertEquals(3L, executeOne(filteredQuery().bin(STR_MAP).mapSize()).getLong(STR_MAP));
        }
    }

    @Nested
    class CdtNavigation {

        @Test
        void mapAndListNavigation() {
            assertEquals("dark",
                executeOne(filteredQuery().bin(SETTINGS).onMapKey("theme").getValues()).getString(SETTINGS));
            assertEquals(200L,
                executeOne(filteredQuery().bin(LONG_MAP).onMapKey(20L).getValues()).getLong(LONG_MAP));
            assertEquals(42L,
                executeOne(filteredQuery().bin(BLOB_MAP).onMapKey(blobKey()).getValues()).getLong(BLOB_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapIndex(1).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapRank(0).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(STR_MAP).onMapValue(20L).getValues()).getValue(STR_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(SETTINGS).onMapValue("dark").getValues()).getValue(SETTINGS));
            List<?> blobValues = executeOne(filteredQuery()
                .bin(BLOB_VAL_MAP).onMapValue(blobValue()).getValues()).getList(BLOB_VAL_MAP);
            assertEquals(1, blobValues.size());
            assertArrayEquals(blobValue(), (byte[]) blobValues.get(0));
            List<?> ratioValues = executeOne(filteredQuery()
                .bin(SETTINGS).onMapValue(1.5).getValues()).getList(SETTINGS);
            assertEquals(1, ratioValues.size());
            assertEquals(1.5, ((Number) ratioValues.get(0)).doubleValue(), 0.001);
            List<?> boolValues = executeOne(filteredQuery()
                .bin(SETTINGS).onMapValue(true).getValues()).getList(SETTINGS);
            assertEquals(1, boolValues.size());
            assertEquals(true, boolValues.get(0));
            assertNotNull(
                executeOne(filteredQuery().bin(STR_MAP).onMapIndexRange(0, 2).getValues()).getValue(STR_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(STR_MAP).onMapIndexRange(1).getValues()).getValue(STR_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapKeyRange(10L, 30L).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(STR_MAP).onMapKeyRange("alpha", "gamma").getValues()).getValue(STR_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapRankRange(0, 2).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapRankRange(1).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(LONG_MAP).onMapValueRange(100L, 250L).getValues()).getValue(LONG_MAP));
            assertNotNull(
                executeOne(filteredQuery().bin(STR_VAL_MAP).onMapValueRange("beta", "delta").getValues())
                    .getValue(STR_VAL_MAP));
            assertNotNull(executeOne(filteredQuery()
                .bin(STR_MAP).onMapKeyList(List.of("alpha", "gamma")).getValues()).getValue(STR_MAP));
            assertNotNull(executeOne(filteredQuery()
                .bin(STR_MAP).onMapValueList(List.of(10L, 30L)).getValues()).getValue(STR_MAP));
            assertEquals(10L,
                executeOne(filteredQuery().bin(SCORES).onListIndex(0).getValues()).getLong(SCORES));
            assertEquals(50L,
                executeOne(filteredQuery().bin(SCORES).onListIndex(4, ListOrder.ORDERED, false).getValues())
                    .getLong(SCORES));
            assertEquals(10L,
                executeOne(filteredQuery().bin(SCORES).onListRank(0).getValues()).getLong(SCORES));
            assertNotNull(
                executeOne(filteredQuery().bin(SCORES).onListValue(20L).getValues()).getValue(SCORES));
            List<?> javaMatches = executeOne(filteredQuery()
                .bin(TAGS).onListValue("java").getValues()).getList(TAGS);
            assertEquals(1, javaMatches.size());
            assertEquals("java", javaMatches.get(0));
            assertNotNull(
                executeOne(filteredQuery().bin(TAGS).onListValue(tagBytes()).getValues()).getValue(TAGS));
            assertEquals(3L,
                executeOne(filteredQuery().bin(NESTED).onMapKey("users").listSize()).getLong(NESTED));
            assertEquals("Bob",
                executeOne(filteredQuery().bin(NESTED).onMapKey("users").listGet(1)).getString(NESTED));
            assertEquals(2,
                executeOne(filteredQuery().bin(NESTED).onMapKey("counts").listGetRange(0, 2)).getList(NESTED).size());
        }

        @Test
        void onEachChildCollectsValues() {
            Record rec = executeOne(filteredQuery()
                .bin(NESTED)
                    .onMapKey("counts")
                    .onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(15)))
                    .collectValues());

            List<?> values = rec.getList(NESTED);
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals(20L, ((Number) values.get(0)).longValue());
            assertEquals(30L, ((Number) values.get(1)).longValue());
        }

        @Test
        void onEachChildAtBinRoot() {
            Record rec = executeOne(filteredQuery()
                .bin(SCORES).onEachChild().collectValues());

            List<?> values = rec.getList(SCORES);
            assertEquals(5, values.size());
            assertEquals(10L, ((Number) values.get(0)).longValue());
        }

        @Test
        void unsupportedAelPathFragmentsThrow() {
            assertThrows(UnsupportedOperationException.class, () ->
                filteredQuery().bin(SCORES).onEachChild("$.x > 0"));
            assertThrows(UnsupportedOperationException.class, () ->
                filteredQuery().bin(SCORES).onEachChild(PreparedAel.prepare("?0 > 0"), 1));
        }
    }

    @Nested
    class SelectFromOverloads {

        @Test
        void typedSelectFromVariants() {
            PreparedAel prepared = PreparedAel.prepare("$." + AGE + ":INT + ?0");
            ExpressionReadOptions opts = new ExpressionReadOptions();
            Expression exp = Exp.build(Exp.add(Exp.intBin(AGE), Exp.val(5)));
            BooleanExpression boolExpr = Ael.longBin(AGE).add(4L);

            Record rec = executeOne(filteredQuery()
                .bin("out_str").selectFrom("$." + AGE + ":INT + 1")
                .bin("out_str_opt").selectFrom("$." + AGE + ":INT + 2", o -> o.ignoreEvalFailure())
                .bin("out_str_direct").selectFrom("$." + AGE + ":INT + 3", new ExpressionReadOptions())
                .bin("out_bool").selectFrom(boolExpr)
                .bin("out_bool_opt").selectFrom(boolExpr, o -> o.ignoreEvalFailure())
                .bin("out_bool_direct").selectFrom(boolExpr, new ExpressionReadOptions())
                .bin("out_prep").selectFrom(prepared, 10)
                .bin("out_prep_opt").selectFrom(prepared, o -> o.ignoreEvalFailure(), 11)
                .bin("out_prep_direct").selectFrom(prepared, opts, 12)
                .bin("out_exp").selectFrom(exp)
                .bin("out_exp_opt").selectFrom(exp, o -> o.ignoreEvalFailure())
                .bin("out_exp_direct").selectFrom(exp, new ExpressionReadOptions())
                .bin("out_expr").selectFrom((Expression) exp)
                .bin("out_expr_opt").selectFrom((Expression) exp, o -> o.ignoreEvalFailure())
                .bin("out_expr_direct").selectFrom((Expression) exp, new ExpressionReadOptions()));

            assertEquals(26L, rec.getLong("out_str"));
            assertEquals(27L, rec.getLong("out_str_opt"));
            assertEquals(28L, rec.getLong("out_str_direct"));
            assertEquals(29L, rec.getLong("out_bool"));
            assertEquals(29L, rec.getLong("out_bool_opt"));
            assertEquals(29L, rec.getLong("out_bool_direct"));
            assertEquals(35L, rec.getLong("out_prep"));
            assertEquals(36L, rec.getLong("out_prep_opt"));
            assertEquals(37L, rec.getLong("out_prep_direct"));
            assertEquals(30L, rec.getLong("out_exp"));
            assertEquals(30L, rec.getLong("out_exp_opt"));
            assertEquals(30L, rec.getLong("out_exp_direct"));
            assertEquals(30L, rec.getLong("out_expr"));
            assertEquals(30L, rec.getLong("out_expr_opt"));
            assertEquals(30L, rec.getLong("out_expr_direct"));
        }
    }

    @Nested
    class HllReads {

        @Test
        void hllProjectionOps() {
            HLLValue otherHll = session.query(seedKey)
                .bin(HLL_OTHER).get()
                .execute()
                .getFirstRecord()
                .getHLLValue(HLL_OTHER);
            List<HLLValue> hlls = List.of(otherHll);

            assertTrue(executeOne(filteredQuery().bin(HLL_BIN).hllGetCount()).getLong(HLL_BIN) >= 3);

            List<?> description = executeOne(filteredQuery().bin(HLL_BIN).hllDescribe())
                .getList(HLL_BIN);
            assertEquals(2, description.size());

            assertTrue(executeOne(filteredQuery().bin(HLL_BIN).hllGetUnion(hlls))
                .getHLLValue(HLL_BIN) != null);

            assertTrue(executeOne(filteredQuery().bin(HLL_BIN).hllGetUnionCount(hlls))
                .getLong(HLL_BIN) >= 3);

            assertTrue(executeOne(filteredQuery().bin(HLL_BIN).hllGetIntersectCount(hlls))
                .getLong(HLL_BIN) >= 2);

            double similarity = executeOne(filteredQuery().bin(HLL_BIN).hllGetSimilarity(hlls))
                .getDouble(HLL_BIN);
            assertTrue(similarity > 0.0 && similarity <= 1.0);
        }
    }

    @Nested
    class BitReads {

        @Test
        void bitProjectionOps() {
            assertArrayEquals(
                new byte[] {(byte) 0xFF},
                executeOne(filteredQuery().bin(BIT_BIN).bitGet(0, 8)).getBytes(BIT_BIN));

            assertTrue(executeOne(filteredQuery().bin(BIT_BIN).bitCount(0, 16))
                .getLong(BIT_BIN) > 0);

            assertEquals(0L, executeOne(filteredQuery().bin(BIT_BIN).bitLscan(0, 16, true))
                .getLong(BIT_BIN));

            assertTrue(executeOne(filteredQuery().bin(BIT_BIN).bitRscan(0, 16, true))
                .getLong(BIT_BIN) >= 0);

            assertEquals(255L, executeOne(filteredQuery().bin(BIT_BIN).bitGetInt(0, 8, false))
                .getLong(BIT_BIN));
        }
    }

    private static byte[] blobKey() {
        return new byte[] {0x01, 0x02};
    }

    private static byte[] blobValue() {
        return new byte[] {0x0A, 0x0B};
    }

    private static byte[] tagBytes() {
        return "python".getBytes();
    }
}
