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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.CdtOperation;
import com.aerospike.client.sdk.cdt.ListOperation;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.cdt.MapOperation;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.MapPolicy;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.cdt.MapWriteFlags;
import com.aerospike.client.sdk.cdt.MapWriteMode;
import com.aerospike.client.sdk.cdt.ModifyFlags;
import com.aerospike.client.sdk.cdt.SelectFlags;
import com.aerospike.client.sdk.cdt.path.CdtCollectOptions;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.util.Packer;
import com.aerospike.client.sdk.util.Version;

/**
 * Integration tests for CDT context helpers ({@link CTX}), {@link CdtOperation} path APIs,
 * and low-level {@link MapOperation} / {@link ListOperation} via {@code appendOperations}.
 */
public class CdtCtxIntegrationTest extends ClusterTest {

    private static final String BIN = "cdtCtx";

    @BeforeAll
    public static void requireCdtPathServer() {
        Version serverVersion = cluster.getRandomNode().getVersion();
        Assumptions.assumeTrue(serverVersion.isGreaterOrEqual(8, 1, 1, 0),
            "CDT path ops require server version 8.1.1+");
    }

    @Test
    public void selectMapKeysInStringValues() {
        Key key = args.set.id("cdtCtxMapKeysInString");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);
        map.put("delta", 40);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX ctx = CTX.mapKeysIn("alpha", "gamma");
        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(10L, 30L), Set.copyOf(values));
    }

    @Test
    public void selectMapKeysInIntKeyValues() {
        Key key = args.set.id("cdtCtxMapKeysInInt");
        session.delete(key).execute();

        Map<Integer, Object> map = new HashMap<>();
        map.put(1, 100L);
        map.put(2, 200L);
        map.put(3, 300L);
        map.put(4, 400L);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX ctx = CTX.mapKeysIn(1, 3);
        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(100L, 300L), Set.copyOf(values));
    }

    @Test
    public void selectMapKeysInWithAndFilter() {
        Key key = args.set.id("cdtCtxMapKeysInAndFilter");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("a", 5);
        map.put("b", 15);
        map.put("c", 25);
        map.put("d", 35);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX keys = CTX.mapKeysIn("a", "b", "c");
        CTX filter = CTX.andFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));

        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.MAP_KEY_VALUE, keys, filter))
            .execute()
            .getFirstRecord();

        // selectByPath MAP_KEY_VALUE returns a list (no map-order extension on the wire),
        // not an AerospikeMap — see CdtMapKeyValueReadOrderTest.
        Map<String, Long> entries = keyValuesFromSelectResult(result.getValue(BIN));
        assertEquals(2, entries.size());
        assertEquals(15L, entries.get("b"));
        assertEquals(25L, entries.get("c"));
    }

    /**
     * Normalizes MAP_KEY_VALUE results from {@code selectByPath} into a plain map for assertions.
     * The server may return a flat {@code [k,v,k,v,...]} list or a single-entry map wrapper.
     */
    private static Map<String, Long> keyValuesFromSelectResult(Object raw) {
        assertNotNull(raw);

        if (raw instanceof AerospikeMap<?, ?> map) {
            Map<String, Long> out = new HashMap<>();
            map.forEach((k, v) -> out.put(k.toString(), ((Number) v).longValue()));
            return out;
        }

        if (raw instanceof AerospikeList<?> list) {
            if (list.size() == 1 && list.get(0) instanceof AerospikeMap<?, ?> sole) {
                return keyValuesFromSelectResult(sole);
            }

            Map<String, Long> out = new HashMap<>();
            if (!list.isEmpty() && list.get(0) instanceof AerospikeList<?>) {
                for (Object item : list) {
                    AerospikeList<?> pair = (AerospikeList<?>) item;
                    out.put(pair.get(0).toString(), ((Number) pair.get(1)).longValue());
                }
                return out;
            }

            for (int i = 0; i + 1 < list.size(); i += 2) {
                out.put(list.get(i).toString(), ((Number) list.get(i + 1)).longValue());
            }
            return out;
        }

        throw new AssertionError("unexpected MAP_KEY_VALUE result type: " + raw.getClass());
    }

    @Test
    public void ctxToBytesRoundTripSelect() {
        Key key = args.set.id("cdtCtxBytesRoundTrip");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX[] original = new CTX[] {CTX.mapKeysIn("alpha", "gamma")};
        CTX[] restored = CTX.fromBytes(CTX.toBytes(original));

        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, restored))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(10L, 30L), Set.copyOf(values));
    }

    @Test
    public void selectMapIndexValue() {
        Key key = args.set.id("cdtCtxMapIndex");
        session.delete(key).execute();

        AerospikeMap<String, Integer> map = AerospikeMap.of(MapOrder.KEY_ORDERED, 4);
        map.put("a", 10);
        map.put("b", 20);
        map.put("c", 30);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX ctx = CTX.mapIndex(1);
        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(20L, ((Number) values.get(0)).longValue());
    }

    @Test
    public void removeMatchesFiltersListElements() {
        Key key = args.set.id("cdtCtxRemoveMatches");
        session.delete(key).execute();

        List<Integer> numbers = new ArrayList<>(List.of(1, 5, 10, 15, 20, 25, 30));
        session.upsert(key).bin(BIN).setTo(numbers).execute();

        session.update(key)
            .bin(BIN)
                .onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)))
                .removeMatches()
            .execute();

        Record result = session.query(key).execute().getFirstRecord();
        List<?> remaining = result.getList(BIN);
        assertNotNull(remaining);
        assertEquals(3, remaining.size());
        assertEquals(List.of(1L, 5L, 10L), remaining);
    }

    @Test
    public void collectValuesWithPrebuiltOptions() {
        Key key = args.set.id("cdtCtxCollectPrebuiltOptions");
        session.delete(key).execute();

        Map<String, Object> data = new HashMap<>();
        data.put("nums", List.of(10, 20, 30));
        session.upsert(key).bin(BIN).setTo(data).execute();

        CdtCollectOptions options = new CdtCollectOptions();
        options.noFail(false);
        Record result = session.query(key)
            .bin(BIN)
                .onMapKey("nums")
                .onEachChild()
                .collectValues(options)
            .execute()
            .getFirstRecord();

        assertEquals(List.of(10L, 20L, 30L), result.getList(BIN));
    }

    @Test
    public void modifyByWithNoFailDisabled() {
        Key key = args.set.id("cdtCtxModifyNoFailDisabled");
        session.delete(key).execute();

        session.upsert(key).bin(BIN).setTo(List.of(5, 15, 25)).execute();

        session.update(key)
            .bin(BIN)
                .onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)))
                .modifyBy(Exp.mul(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(2)), o -> o.noFail(false))
            .execute();

        Record result = session.query(key).execute().getFirstRecord();
        assertEquals(List.of(5L, 30L, 50L), result.getList(BIN));
    }

    @Test
    public void removeMatchesNoFailOnEmptyMatch() {
        Key key = args.set.id("cdtCtxRemoveMatchesNoFail");
        session.delete(key).execute();

        session.upsert(key).bin(BIN).setTo(List.of(1, 2, 3)).execute();

        session.update(key)
            .bin(BIN)
                .onEachChild(Exp.lt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0)))
                .removeMatches(o -> o.noFail(true))
            .execute();

        Record result = session.query(key).execute().getFirstRecord();
        List<?> remaining = result.getList(BIN);
        assertNotNull(remaining);
        assertEquals(3, remaining.size());
    }

    @Test
    public void modifyByWithNoFailOption() {
        Key key = args.set.id("cdtCtxModifyByNoFail");
        session.delete(key).execute();

        session.upsert(key).bin(BIN).setTo(List.of(5, 15, 25)).execute();

        session.update(key)
            .bin(BIN)
                .onEachChild(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)))
                .modifyBy(Exp.mul(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(2)), o -> o.noFail(true))
            .execute();

        Record result = session.query(key).execute().getFirstRecord();
        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals(5L, ((Number) values.get(0)).longValue());
        assertEquals(30L, ((Number) values.get(1)).longValue());
        assertEquals(50L, ((Number) values.get(2)).longValue());
    }

    @Test
    public void mapOperationGetByKeyListViaAppendOperations() {
        Key key = args.set.id("cdtCtxMapOpGetByKeyList");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("a", 1L);
        map.put("b", 2L);
        map.put("c", 3L);
        session.upsert(key).bin(BIN).setTo(map).execute();

        Record result = session.query(key)
            .appendOperations(MapOperation.getByKeyList(
                BIN,
                List.of(Value.get("a"), Value.get("c")),
                MapReturnType.VALUE))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(1L, 3L), Set.copyOf(values));
    }

    @Test
    public void listOperationGetByIndexRangeViaAppendOperations() {
        Key key = args.set.id("cdtCtxListOpGetByIndexRange");
        session.delete(key).execute();

        session.upsert(key).bin(BIN).setTo(List.of(10, 20, 30, 40, 50)).execute();

        Record result = session.query(key)
            .appendOperations(ListOperation.getByIndexRange(BIN, 1, 3, ListReturnType.VALUE))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(3, values.size());
        assertEquals(List.of(20L, 30L, 40L), values);
    }

    @Test
    public void mapOperationIncrementViaAppendOperations() {
        Key key = args.set.id("cdtCtxMapOpIncrement");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("count", 10L);
        session.upsert(key).bin(BIN).setTo(map).execute();

        session.upsert(key)
            .appendOperations(MapOperation.increment(MapPolicy.Default, BIN, Value.get("count"), Value.get(-3)))
            .execute();

        Record result = session.query(key).execute().getFirstRecord();
        AerospikeMap<?, ?> updated = result.getMap(BIN);
        assertNotNull(updated);
        assertEquals(7L, ((Number) updated.get("count")).longValue());
    }

    @Test
    public void selectMapKeysInLongKeys() {
        Key key = args.set.id("cdtCtxMapKeysInLong");
        session.delete(key).execute();

        Map<Long, Object> map = new LinkedHashMap<>();
        map.put(1L, 100L);
        map.put(2L, 200L);
        map.put(3L, 300L);
        session.upsert(key).bin(BIN).setTo(map).execute();

        CTX ctx = CTX.mapKeysIn(1L, 3L);
        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, ctx))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(100L, 300L), Set.copyOf(values));
    }

    @Test
    public void ctxBase64RoundTripWithCompiledAndFilter() {
        Key key = args.set.id("cdtCtxBase64AndFilter");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("a", 5);
        map.put("b", 15);
        map.put("c", 25);
        session.upsert(key).bin(BIN).setTo(map).execute();

        Expression compiled = Exp.build(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10)));
        CTX[] original = new CTX[] {
            CTX.mapKeysIn("a", "b", "c"),
            CTX.andFilter(compiled)
        };
        CTX[] restored = CTX.fromBase64(CTX.toBase64(original));

        Record result = session.query(key)
            .appendOperations(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, restored))
            .execute()
            .getFirstRecord();

        List<?> values = result.getList(BIN);
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals(Set.of(15L, 25L), Set.copyOf(values));
    }

    @Test
    public void mapLowLevelOperationsViaAppendOperations() {
        Key key = args.set.id("cdtMapLowLevelOps");
        session.delete(key).execute();

        session.upsert(key)
            .appendOperations(MapOperation.create(BIN, MapOrder.UNORDERED))
            .execute();
        session.delete(key).execute();

        session.upsert(key)
            .appendOperations(MapOperation.create(BIN, MapOrder.KEY_ORDERED, true))
            .execute();

        Map<Value, Value> items = new HashMap<>();
        items.put(Value.get("a"), Value.get(10L));
        items.put(Value.get("b"), Value.get(20L));
        items.put(Value.get("c"), Value.get(30L));
        items.put(Value.get("d"), Value.get(40L));
        session.upsert(key)
            .appendOperations(MapOperation.putItems(MapPolicy.Default, BIN, items))
            .execute();

        MapPolicy replaceItemsPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE_ONLY);
        Map<Value, Value> updates = Map.of(Value.get("a"), Value.get(12L));
        session.upsert(key)
            .appendOperations(MapOperation.putItems(replaceItemsPolicy, BIN, updates))
            .execute();
        assertEquals(12L, session.query(key).execute().getFirstRecord().getMap(BIN).get("a"));

        Record byIndex = session.query(key)
            .appendOperations(MapOperation.getByIndexRange(BIN, 1, MapReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(20L, 30L, 40L), byIndex.getList(BIN));

        Record byRank = session.query(key)
            .appendOperations(MapOperation.getByRankRange(BIN, 1, MapReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(20L, 30L, 40L), byRank.getList(BIN));

        session.upsert(key)
            .appendOperations(MapOperation.removeByIndex(BIN, 0, MapReturnType.VALUE))
            .execute();
        assertEquals(3, session.query(key).execute().getFirstRecord().getMap(BIN).size());

        session.upsert(key)
            .appendOperations(MapOperation.removeByValueList(
                BIN, List.of(Value.get(20L), Value.get(40L)), MapReturnType.COUNT))
            .execute();
        assertEquals(1, session.query(key).execute().getFirstRecord().getMap(BIN).size());

        session.upsert(key)
            .appendOperations(MapOperation.removeByRank(BIN, 0, MapReturnType.VALUE))
            .execute();
        assertEquals(0, session.query(key).execute().getFirstRecord().getMap(BIN).size());

        session.upsert(key)
            .appendOperations(MapOperation.putItems(MapPolicy.Default, BIN, items))
            .execute();
        session.upsert(key)
            .appendOperations(MapOperation.removeByIndexRange(BIN, 2, MapReturnType.VALUE))
            .execute();
        assertEquals(2, session.query(key).execute().getFirstRecord().getMap(BIN).size());

        session.upsert(key)
            .appendOperations(MapOperation.putItems(MapPolicy.Default, BIN, items))
            .execute();
        session.upsert(key)
            .appendOperations(MapOperation.removeByRankRange(BIN, 2, MapReturnType.VALUE))
            .execute();
        assertEquals(2, session.query(key).execute().getFirstRecord().getMap(BIN).size());

        MapPolicy writeModePolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY);
        session.upsert(key)
            .appendOperations(MapOperation.put(writeModePolicy, BIN, Value.get("a"), Value.get(11L)))
            .execute();
        assertEquals(11L, session.query(key).execute().getFirstRecord().getMap(BIN).get("a"));

        MapPolicy flaggedPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.NO_FAIL);
        session.upsert(key)
            .appendOperations(MapOperation.put(flaggedPolicy, BIN, Value.get("z"), Value.get(99L)))
            .execute();
        assertEquals(99L, session.query(key).execute().getFirstRecord().getMap(BIN).get("z"));

        session.upsert(key)
            .appendOperations(MapOperation.decrement(MapPolicy.Default, BIN, Value.get("z"), Value.get(4)))
            .execute();
        assertEquals(95L, session.query(key).execute().getFirstRecord().getMap(BIN).get("z"));

        Map<String, Object> nested = new HashMap<>();
        nested.put("inner", new HashMap<String, Object>());
        session.upsert(key).bin(BIN).setTo(nested).execute();
        MapPolicy persistPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT, true);
        session.upsert(key)
            .appendOperations(MapOperation.setMapPolicy(persistPolicy, BIN, CTX.mapKey(Value.get("inner"))))
            .appendOperations(MapOperation.create(BIN, MapOrder.UNORDERED, CTX.mapKey(Value.get("inner"))))
            .execute();

        session.delete(key).execute();
        MapPolicy partialPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.PARTIAL);
        session.upsert(key)
            .appendOperations(MapOperation.putItems(MapPolicy.Default, BIN, items))
            .appendOperations(MapOperation.putItems(partialPolicy, BIN, Map.of(Value.get("e"), Value.get(50L))))
            .execute();
        assertEquals(5, session.query(key).execute().getFirstRecord().getMap(BIN).size());
    }

    @Test
    public void listLowLevelOperationsViaAppendOperations() {
        Key key = args.set.id("cdtListLowLevelOps");
        session.delete(key).execute();

        session.upsert(key).bin(BIN).setTo(List.of(10, 20, 30, 40, 50)).execute();

        Record byIndex = session.query(key)
            .appendOperations(ListOperation.getByIndexRange(BIN, 2, 3, ListReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(30L, 40L, 50L), byIndex.getList(BIN));

        Record byRank = session.query(key)
            .appendOperations(ListOperation.getByRankRange(BIN, 2, 3, ListReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(30L, 40L, 50L), byRank.getList(BIN));

        session.delete(key).execute();
        session.upsert(key)
            .appendOperations(ListOperation.append(BIN, Value.get(10)))
            .appendOperations(ListOperation.append(BIN, Value.get(20)))
            .appendOperations(ListOperation.append(BIN, Value.get(30)))
            .appendOperations(ListOperation.append(BIN, Value.get(40)))
            .appendOperations(ListOperation.append(BIN, Value.get(50)))
            .execute();

        session.upsert(key)
            .appendOperations(ListOperation.insertItems(BIN, 1, List.of(Value.get(15))))
            .execute();
        session.upsert(key)
            .appendOperations(ListOperation.increment(BIN, 0))
            .appendOperations(ListOperation.increment(BIN, 2, Value.get(5)))
            .appendOperations(ListOperation.set(BIN, 3, Value.get(99)))
            .execute();
        assertEquals(List.of(11L, 15L, 25L, 99L, 40L, 50L), session.query(key).execute().getFirstRecord().getList(BIN));

        session.delete(key).execute();
        session.upsert(key)
            .appendOperations(ListOperation.append(BIN, Value.get(10)))
            .appendOperations(ListOperation.append(BIN, Value.get(20)))
            .appendOperations(ListOperation.append(BIN, Value.get(30)))
            .appendOperations(ListOperation.append(BIN, Value.get(40)))
            .appendOperations(ListOperation.append(BIN, Value.get(50)))
            .execute();
        session.upsert(key)
            .appendOperations(ListOperation.removeRange(BIN, 4))
            .execute();
        assertEquals(4, session.query(key).execute().getFirstRecord().getList(BIN).size());

        session.delete(key).execute();
        session.upsert(key)
            .appendOperations(ListOperation.append(BIN, Value.get(10)))
            .appendOperations(ListOperation.append(BIN, Value.get(20)))
            .appendOperations(ListOperation.append(BIN, Value.get(30)))
            .appendOperations(ListOperation.append(BIN, Value.get(40)))
            .appendOperations(ListOperation.append(BIN, Value.get(50)))
            .execute();
        session.upsert(key)
            .appendOperations(ListOperation.removeRange(BIN, 2, 3))
            .execute();
        assertEquals(2, session.query(key).execute().getFirstRecord().getList(BIN).size());

        Map<String, Object> root = new HashMap<>();
        root.put("items", new ArrayList<Integer>());
        session.upsert(key).bin(BIN).setTo(root).execute();
        session.upsert(key)
            .appendOperations(ListOperation.create(BIN, ListOrder.UNORDERED, false, CTX.mapKey(Value.get("items"))))
            .execute();
    }

    @Test
    public void selectByPathAndModifyByPathRejectInvalidBinNames() {
        Expression modifyExp = Exp.build(Exp.mul(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(2)));
        String tooLong = "x".repeat(Bin.MAX_BIN_NAME_LENGTH + 1);

        assertThrows(AerospikeException.class,
            () -> CdtOperation.selectByPath(null, SelectFlags.VALUE));
        assertThrows(AerospikeException.class,
            () -> CdtOperation.selectByPath("", SelectFlags.VALUE));
        assertThrows(AerospikeException.class,
            () -> CdtOperation.selectByPath(tooLong, SelectFlags.VALUE));

        assertThrows(AerospikeException.class,
            () -> CdtOperation.modifyByPath(null, ModifyFlags.DEFAULT, modifyExp));
        assertThrows(AerospikeException.class,
            () -> CdtOperation.modifyByPath("", ModifyFlags.DEFAULT, modifyExp));
        assertThrows(AerospikeException.class,
            () -> CdtOperation.modifyByPath(tooLong, ModifyFlags.DEFAULT, modifyExp));
    }

    @Test
    public void selectByPathAndModifyByPathAcceptEmptyContext() {
        assertNotNull(CdtOperation.selectByPath(BIN, SelectFlags.VALUE));
        assertNotNull(CdtOperation.selectByPath(BIN, SelectFlags.VALUE, new CTX[0]));

        Expression modifyExp = Exp.build(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(1)));
        assertNotNull(CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, modifyExp));
        assertNotNull(CdtOperation.modifyByPath(BIN, ModifyFlags.DEFAULT, modifyExp, new CTX[0]));
    }

    @Test
    public void ctxMapKeysInByteShortAndFloatKeysRoundTrip() {
        CTX[] contexts = new CTX[] {
            CTX.mapKeysIn((byte) 1, (byte) 2),
            CTX.mapKeysIn((short) 3, (short) 4),
            CTX.mapKeysIn(1.5, 2.5),
            CTX.mapKeysIn(3.5f, 4.5f),
        };

        for (CTX original : contexts) {
            CTX[] restored = CTX.fromBytes(CTX.toBytes(new CTX[] {original}));
            assertEquals(1, restored.length);
            assertEquals(original.id, restored[0].id);
        }
    }

    @Test
    public void ctxFromBytesRejectsTruncatedContextPayload() {
        Packer packer = new Packer();
        for (int pass = 0; pass < 2; pass++) {
            packer.packArrayBegin(1);
            packer.packInt(0x20);
            if (pass == 0) {
                packer.createBuffer();
            }
        }
        byte[] malformed = packer.getBuffer();

        assertThrows(AerospikeException.Parse.class, () -> CTX.fromBytes(malformed));
    }

    @Test
    public void mapOperationOpenEndedKeyAndValueRanges() {
        Key key = args.set.id("cdtCtxMapOpenRanges");
        session.delete(key).execute();

        Map<String, Object> map = new HashMap<>();
        map.put("a", 1L);
        map.put("b", 2L);
        map.put("c", 3L);
        map.put("d", 4L);
        session.upsert(key).bin(BIN).setTo(map).execute();

        Record ltC = session.query(key)
            .appendOperations(MapOperation.getByKeyRange(BIN, null, Value.get("c"), MapReturnType.KEY))
            .execute()
            .getFirstRecord();
        assertEquals(Set.of("a", "b"), Set.copyOf(ltC.getList(BIN)));

        Record gteB = session.query(key)
            .appendOperations(MapOperation.getByKeyRange(BIN, Value.get("b"), null, MapReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(Set.of(2L, 3L, 4L), Set.copyOf(gteB.getList(BIN)));

        Record valueWindow = session.query(key)
            .appendOperations(MapOperation.getByValueRange(BIN, Value.get(2L), Value.get(4L), MapReturnType.KEY))
            .execute()
            .getFirstRecord();
        assertEquals(Set.of("b", "c"), Set.copyOf(valueWindow.getList(BIN)));
    }

    @Test
    public void listOperationOpenEndedValueRanges() {
        Key key = args.set.id("cdtCtxListOpenRanges");
        session.delete(key).execute();
        session.upsert(key)
            .appendOperations(ListOperation.create(BIN, ListOrder.ORDERED, false, false))
            .appendOperations(ListOperation.append(BIN, Value.get(10)))
            .appendOperations(ListOperation.append(BIN, Value.get(20)))
            .appendOperations(ListOperation.append(BIN, Value.get(30)))
            .appendOperations(ListOperation.append(BIN, Value.get(40)))
            .appendOperations(ListOperation.append(BIN, Value.get(50)))
            .execute();

        Record closed = session.query(key)
            .appendOperations(ListOperation.getByValueRange(BIN, Value.get(15), Value.get(45), ListReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(20L, 30L, 40L), closed.getList(BIN));

        Record gte20 = session.query(key)
            .appendOperations(ListOperation.getByValueRange(BIN, Value.get(20), null, ListReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(20L, 30L, 40L, 50L), gte20.getList(BIN));

        Record lt30 = session.query(key)
            .appendOperations(ListOperation.getByValueRange(BIN, null, Value.get(30), ListReturnType.VALUE))
            .execute()
            .getFirstRecord();
        assertEquals(List.of(10L, 20L), lt30.getList(BIN));
    }

    @Test
    public void mapOperationSetMapPolicyAndUpdateOnlyPut() {
        Key key = args.set.id("cdtCtxMapPolicyOps");
        session.delete(key).execute();

        session.upsert(key)
            .appendOperations(MapOperation.create(BIN, MapOrder.KEY_ORDERED, false))
            .appendOperations(MapOperation.putItems(MapPolicy.Default, BIN,
                Map.of(Value.get("a"), Value.get(1L))))
            .execute();

        MapPolicy noPersistIndex = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT, false);
        session.upsert(key)
            .appendOperations(MapOperation.setMapPolicy(noPersistIndex, BIN))
            .execute();

        MapPolicy replacePolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE_ONLY);
        session.upsert(key)
            .appendOperations(MapOperation.put(replacePolicy, BIN, Value.get("a"), Value.get(9L)))
            .execute();
        assertEquals(9L, session.query(key).execute().getFirstRecord().getMap(BIN).get("a"));

        Map<String, Object> nested = new HashMap<>();
        nested.put("inner", new HashMap<String, Object>());
        session.upsert(key).bin(BIN).setTo(nested).execute();
        session.upsert(key)
            .appendOperations(MapOperation.setMapPolicy(MapPolicy.Default, BIN, CTX.mapKey(Value.get("inner"))))
            .appendOperations(MapOperation.create(BIN, MapOrder.UNORDERED, false, CTX.mapKey(Value.get("inner"))))
            .execute();
    }

    @Test
    public void listOperationCreateInNestedMapAndSetOrder() {
        Key key = args.set.id("cdtCtxListPolicyOps");
        session.delete(key).execute();

        Map<String, Object> listRoot = new HashMap<>();
        listRoot.put("items", new ArrayList<Integer>());
        session.upsert(key).bin(BIN).setTo(listRoot).execute();
        session.upsert(key)
            .appendOperations(ListOperation.create(BIN, ListOrder.UNORDERED, true, CTX.mapKey(Value.get("items"))))
            .execute();

        session.delete(key).execute();
        session.upsert(key)
            .appendOperations(ListOperation.create(BIN, ListOrder.ORDERED, false, false))
            .appendOperations(ListOperation.setOrder(BIN, ListOrder.ORDERED, false))
            .appendOperations(ListOperation.append(BIN, Value.get(7)))
            .execute();
        assertTrue(session.query(key).execute().getFirstRecord().getList(BIN).contains(7L));
    }
}
