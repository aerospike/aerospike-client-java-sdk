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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.junit.RequiresServerFeature;
import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ResolvedSettings;

@RequiresServerFeature(ServerFeature.EXTENDED_ERROR_DETAIL)
public class ErrorDetailVerbosityTest extends ClusterTest {
    private static final String binName = "edv-bin";
    private static Key intKey;
    private static Key strKey;
    private static Key listKey;

    @BeforeAll
    public static void setup() {
        intKey = args.set.id("edv-int-key");
        strKey = args.set.id("edv-str-key");
        listKey = args.set.id("edv-list-key");

        session.upsert(intKey)
            .bin(binName).setTo(1)
            .execute();

        session.upsert(strKey)
            .bin(binName).setTo("hello")
            .execute();

        List<Integer> seed = new ArrayList<>();
        seed.add(10);
        seed.add(20);
        seed.add(30);

        session.upsert(listKey)
            .bin(binName).setTo(seed)
            .execute();
    }

    // ---------------------------------------------------------------------
    // Verbosity level semantics.
    // ---------------------------------------------------------------------

    @Test
    public void testDefaultVerbosityIsZero() {
        ResolvedSettings settings = session.getBehavior().getSettings(OpKind.READ, OpShape.POINT, Behavior.Mode.AP);
        assertEquals(0, settings.getErrorDetailVerbosity());

        settings = session.getBehavior().getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Behavior.Mode.AP);
        assertEquals(0, settings.getErrorDetailVerbosity());
    }

    @Test
    public void testVerbosityDisabled() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.NONE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .bin(binName).append("bad")
                .execute()
                .getFirstRecord();
        });

        assertEquals(ResultCode.BIN_TYPE_ERROR, ae.getResultCode());

        // With verbosity 0, the message should be the default ResultCode string.
        String msg = ae.getBaseMessage();
        assertEquals(ResultCode.getResultString(ResultCode.BIN_TYPE_ERROR), msg);
    }

    @Test
    public void testVerbositySubCodeOnly() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.SUBCODE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-subonly-key");

        session1.upsert(key)
            .bin("other-bin").setTo(1)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin("no-hll-bin").hllRefreshCount()
                .execute()
                .getFirstRecord();
        });

        assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());
        assertEquals(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.getSubCode());
        String msg = ae.getBaseMessage();
        assertNotNull(msg);
    }

    @Test
    public void testVerbositySubCodeAndMessage() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-submsg-key");

        session1.upsert(key)
            .bin("other-bin").setTo(1)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin("no-hll-bin").hllRefreshCount()
                .execute()
                .getFirstRecord();
        });

        assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());
        assertEquals(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.getSubCode());
        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("count op"), "Expected message text in: " + msg);
    }

    // ---------------------------------------------------------------------
    // SubCode-absent cases (AS_SUB_NONE): the status is already maximally
    // specific, so the server omits the subcode map key and the client must
    // never format a "(subcode=...)" suffix. The message carries the context.
    // ---------------------------------------------------------------------

    @Test
    public void testAppendToIntegerBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .bin(binName).append("bad-append")
                .execute()
                .getFirstRecord();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "string_append requires string bin");
    }

    @Test
    public void testIncrementStringBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(strKey)
                .bin(binName).add(1)
                .execute()
                .getFirstRecord();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "cannot increment");
    }

    @Test
    public void testHllAddOnIntegerBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        List<Value> hllList = new ArrayList<>();
        hllList.add(Value.get("element1"));

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .bin(binName).hllAdd(hllList, HllConfig.of(8))
                .execute()
                .getFirstRecord();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "bin is not hll type");
    }

    @Test
    @Tag(KnownDefect.TAG)
    public void testDeleteGenerationMismatch() {
        KnownDefect.skipWhere(args.scMode,
            "on a strong-consistency namespace the delete is durable, so it runs through tombstone_master in"
                + " delete_ee.c rather than delete.c. Both do the same generation_check and set the same"
                + " AS_ERR_GENERATION, but the Enterprise path omits the matching"
                + " as_error_details_set_fmt(AS_SUB_NONE, \"delete generation mismatch\"), so the server sends"
                + " no detail and the client falls back to the generic ResultCode text. The fix belongs in the"
                + " server; see docs/strong-consistency-8.1.3-findings.md");

        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.delete(intKey)
                .ensureGenerationIs(777)
                .execute()
                .getFirstRecord();
        });

        assertSubCodeAbsent(ae, ResultCode.GENERATION_ERROR, "generation");
    }

    // ---------------------------------------------------------------------
    // SubCode-present cases: per-status enum subcode numbering.
    // ---------------------------------------------------------------------

    @Test
    public void testHllRefreshCountMissingBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-no-hll-key");

        session1.upsert(key)
            .bin("other-bin").setTo(1)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin("no-hll-bin").hllRefreshCount()
                .execute()
                .getFirstRecord();
        });

        // AS_SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP = 1
        assertSubCode(ae, ResultCode.BIN_NOT_FOUND, SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP);
    }

    @Test
    public void testListGetIndexOutOfBounds() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).listGet(99)
                .execute()
                .getFirstRecord();
        });

        // AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1
        assertSubCode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
    }

    @Test
    public void testListGetByRankOutOfBounds() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).onListRank(99).getValues()
                .execute()
                .getFirstRecord();
        });

        // AS_SUB_OPNOT_CDT_RANK_OUT_OF_BOUNDS = 2
        assertSubCode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_RANK_OUT_OF_BOUNDS);
    }

    @Test
    public void testListBoundedOverflow() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).listInsert(10, 5, opts -> opts .insertBounded())
                .execute()
                .getFirstRecord();
        });

        // AS_SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW = 3
        assertSubCode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW);
    }

    @Test
    public void testHllFoldTargetTooLarge() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-hll-fold-key");

        session1.delete(key)
            .execute();

        session1.upsert(key)
            .bin(binName).hllInit(HllConfig.of(8))
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).hllFold(14)
                .execute();
        });

        // AS_SUB_OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE = 8
        assertSubCode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE);
    }

    @Test
    public void testBitGetOffsetOutOfRange() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-bits-key");

        session1.upsert(key)
            .bin(binName).setTo(new byte[] {(byte)0xAA, (byte)0xBB, (byte)0xCC, (byte)0xDD})
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).bitGet(2000000000, 8)
                .execute();
        });

        // AS_SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE = 2
        assertSubCode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_OFFSET_OUT_OF_RANGE);
    }

    @Test
    public void testBitGetSizeZero() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-bits-key2");

        session1.upsert(key)
            .bin(binName).setTo(new byte[] {(byte)0xAA, (byte)0xBB, (byte)0xCC, (byte)0xDD})
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).bitGet(0, 0)
                .execute();
        });

        // AS_SUB_PARAM_BITS_SIZE_OUT_OF_RANGE = 3
        assertSubCode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_SIZE_OUT_OF_RANGE);
    }

    @Test
    public void testReadFilteredOut() {
        // FILTERED_OUT carries no subcode (AS_SUB_NONE) and a contextual message;
        // the server's as_sub_filtered_t enum was removed, so there is no version gate.
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(Exp.eq(Exp.intBin(binName), Exp.val(99)))
                .failOnFilteredOut()
                .execute()
                .getFirstRecord();
        });

        assertSubCodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
    }

    // ---------------------------------------------------------------------
    // Additional particle modify type mismatches (subcode absent).
    // ---------------------------------------------------------------------

    @Test
    public void testPrependToIntegerBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .bin(binName).prepend("bad-prepend")
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "prepend");
    }

    @Test
    public void testIncrementDoubleOnIntegerBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(strKey)
                .bin(binName).add(1.5)
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    // ---------------------------------------------------------------------
    // Additional CDT list ops.
    // ---------------------------------------------------------------------

    @Test
    public void testListPopIndexOutOfBounds() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).listPop(99)
                .execute();
        });

        // AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1
        assertSubCode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
    }

    @Test
    public void testListAddUniqueViolation() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).listAppend(20, ops -> ops .addUnique())
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.ELEMENT_EXISTS);
    }

    @Test
    public void testListOpOnRawBytesBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        // A raw-bytes bin is not a list -> list_get triggers the wrong-type path.
        Key key = args.set.id("edv-list-raw-key");

        session1.upsert(key)
            .bin(binName).setTo(new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF})
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).listGet(0)
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    // ---------------------------------------------------------------------
    // CDT map ops.
    // ---------------------------------------------------------------------

    @Test
    public void testMapCreateOnlyExistingKey() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-map-create-key");

        Map<Integer,String> seed = new HashMap<>();
        seed.put(1, "a");

        session1.upsert(key)
            .bin(binName).setTo(seed)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).onMapKey(1).insert("b")
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.ELEMENT_EXISTS);
    }

    @Test
    public void testMapUpdateOnlyMissingKey() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-map-update-key");
        Map<Integer,String> seed = new HashMap<>();
        seed.put(1, "a");

        session1.upsert(key)
            .bin(binName).setTo(seed)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).onMapKey(99).update("b")
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.ELEMENT_NOT_FOUND);
    }

    @Test
    public void testMapOpOnListBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        // listKey holds a list; a map op against it triggers the wrong-type path.
        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(listKey)
                .bin(binName).onMapKey(1).getAsMap()
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    @Test
    public void testMapOpOnRawBytesBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-map-raw-key");

        session1.upsert(key)
            .bin(binName).setTo(new byte[]{0x42, 0x42})
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).onMapKey(1).getAsMap()
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    @Test
    public void testListCtxIntoStringMapValue() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        // Map value at key 1 is a string; descending into it with a list op and
        // a map-key context is a type mismatch.
        Key key = args.set.id("edv-map-ctx-key");
        Map<Integer,String> seed = new HashMap<>();
        seed.put(1, "leaf-string");

        session1.upsert(key)
            .bin(binName).setTo(seed)
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).onMapKey(1).onListIndex(0).getValues()
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    // ---------------------------------------------------------------------
    // Additional HLL ops.
    // ---------------------------------------------------------------------

    @Test
    public void testHllInitInvalidIndexBits() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-hll-bad-bits-key");

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            // Index bit count out of the legal [4,16] range -> server-side reject.
            session1.upsert(key)
                .bin(binName).hllInit(HllConfig.of(30))
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.PARAMETER_ERROR);
    }

    @Test
    public void testHllOpOnRawBytesBin() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-hll-raw-key");

        session1.upsert(key)
            .bin(binName).setTo(new byte[]{0x01, 0x02, 0x03})
            .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(key)
                .bin(binName).hllGetCount()
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
    }

    // ---------------------------------------------------------------------
    // Write / delete / read policy (subcode absent unless noted).
    // ---------------------------------------------------------------------

    @Test
    public void testWriteCreateOnlyExistingRecord() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            // intKey already exists.
            session1.insert(intKey)
                .bin(binName).setTo(2)
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.KEY_EXISTS_ERROR);
    }

    @Test
    public void testWriteReplaceOnlyMissingRecord() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-replace-missing-key");

        session1.delete(key).execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.replaceIfExists(key)
                .bin(binName).setTo(1)
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.KEY_NOT_FOUND_ERROR);
    }

    @Test
    public void testWriteGenerationMismatch() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .ensureGenerationIs(999)
                .bin(binName).setTo(2)
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.GENERATION_ERROR, "generation");
    }

    @Test
    public void testOperateFilteredOut() {
        // FILTERED_OUT carries no subcode (AS_SUB_NONE) and a contextual message;
        // the server's as_sub_filtered_t enum was removed, so there is no version gate.
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);
        Exp exp = Exp.eq(Exp.intBin(binName), Exp.val(99));

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(exp)
                .failOnFilteredOut()
                .execute();
        });

        assertSubCodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
    }

    // ---------------------------------------------------------------------
    // Happy path: verbosity set on a successful command must not break.
    // ---------------------------------------------------------------------

    @Test
    public void testSuccessNoErrorDetails() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Key key = args.set.id("edv-success-key");

        session1.upsert(key)
            .bin(binName).setTo(42)
            .execute();

        Record rec = session1.query(key)
            .execute()
            .getFirstRecord();

        assertNotNull(rec);
        assertEquals(42, rec.getInt(binName));
    }

    // ---------------------------------------------------------------------
    // Verbosity 3: expression build-failure trace (SERVER-1137).
    //
    // A type-mismatched comparison expression fails to *build* on the server.
    // As a filter_exp it yields "invalid metadata expression in request"; as an
    // exp_write op it yields "invalid expression in operation request". Both carry
    // PARAMETER_ERROR + SubCode.NONE and, at verbosity 3, a structured build trace.
    // Assert trace PRESENCE and SHAPE, not exact byte_offset/snippet bytes.
    // ---------------------------------------------------------------------

    /** Expression whose operands are type-mismatched (int vs float), so the server build fails. */

    private static Exp badExp() {
        return Exp.eq(Exp.val(5), Exp.val(6.0));
    }

    @Test
    public void testFilterExpBuildFailureTrace() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE)
            )
        );

        Session session1 = cluster.createSession(behavior1);

        Exp exp = badExp();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(exp)
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("invalid filter expression in request"),
            "Expected filter-build message in: " + msg);

        ExpressionTrace t = ae.getExpressionTrace();
        assertNotNull(t, "Expected a non-null expression trace at verbosity 3");
        assertEquals(ExpressionTrace.PHASE_BUILD, t.getPhase(), "Expected a build-phase trace");
    }

    @Test
    public void testExpWriteBuildFailureTrace() {
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE)
            )
        );

        Session session1 = cluster.createSession(behavior1);
        Exp exp = badExp();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.upsert(intKey)
                .bin(binName).upsertFrom(exp)
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);
        assertTrue(msg.contains("invalid expression in operation request"),
            "Expected exp-op build message in: " + msg);

        ExpressionTrace t = ae.getExpressionTrace();
        assertNotNull(t, "Expected a non-null expression trace at verbosity 3");
        assertEquals(ExpressionTrace.PHASE_BUILD, t.getPhase(), "Expected a build-phase trace");
    }

    @Test
    public void testFilterExpBuildFailureVerbosity2HasNoTrace() {
        // Additive-superset check: the SAME inducer at verbosity 2 surfaces the same
        // message but NO trace. Verbosity 3 = verbosity 2 + trace.
        Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.MESSAGE)
            )
        );

        Session session1 = cluster.createSession(behavior1);
        Exp exp = badExp();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            session1.query(intKey)
                .where(exp)
                .execute();
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
        assertEquals(SubCode.NONE, ae.getSubCode());

        String msg = ae.getBaseMessage();
        assertNotNull(msg);

        assertTrue(msg.contains("invalid filter expression in request"),
            "Expected filter-build message in: " + msg);

        assertNull(ae.getExpressionTrace(), "Verbosity 2 must surface NO expression trace");
    }

    /**
     * Assert the server-supplied {@code (resultCode, subcode)} pair. The numeric
     * subcode must be exposed first-class via {@link AerospikeException#getSubCode()}
     * (not merely embedded in the message string).
     */
    private void assertSubCode(AerospikeException ae, int expectedResultCode, int expectedSubCode) {
        assertEquals(expectedResultCode, ae.getResultCode(), "Unexpected result code");
        assertEquals(expectedSubCode, ae.getSubCode(), "Unexpected subcode");

        String msg = ae.getBaseMessage();
        assertNotNull(msg, "Expected server error message");
    }

    /**
     * Assert that the server surfaced a contextual message but NO subcode
     * (AS_SUB_NONE): {@link AerospikeException#getSubCode()} is {@link SubCode#NONE}
     * and the "(subcode=...)" suffix must never appear. Any expectedSubstrings are
     * required in the message; pass none to skip the message-text check (mirrors a
     * NULL expected_msg_substr in the C example).
     */
    private void assertSubCodeAbsent(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
        assertEquals(expectedResultCode, ae.getResultCode(), "Unexpected result code");
        assertEquals(SubCode.NONE, ae.getSubCode(), "Expected no subcode");

        String msg = ae.getBaseMessage();
        assertNotNull(msg, "Expected server error message");

        for (String expected : expectedSubstrings) {
            assertTrue(msg.contains(expected), "Expected '" + expected + "' in: " + msg);
        }
    }
}
