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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.query.IndexType;
import com.aerospike.client.sdk.task.ExecuteTask;
import com.aerospike.client.sdk.task.RegisterTask;

public class DurableDeleteTests extends ClusterTest {

    private static final String ddUdfIndexName = "ddudfinx";
    private static final String ddUdfKeyPrefix = "ddudfk";
    private static final String ddUdfBin1 = "ddux1";
    private static final String ddUdfBin2 = "ddux2";
    private static final int ddUdfSize = 10;

    @BeforeAll
    public static void prepareDdUdfBackgroundDelete() {
        RegisterTask reg = session.registerUdfString(UdfTest.lua, "record_example.lua");
        reg.waitTillComplete();

        try {
            session.createIndex(args.set, ddUdfIndexName, ddUdfBin1, IndexType.INTEGER,
                IndexCollectionType.DEFAULT)
                .waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        for (int i = 1; i <= ddUdfSize; i++) {
            Key key = args.set.id(ddUdfKeyPrefix + i);
            session.upsert(key)
                .bins(ddUdfBin1, ddUdfBin2)
                .values(i, i)
                .execute();
        }
    }

    @AfterAll
    public static void tearDownDdUdfBackgroundDelete() {
        session.dropIndex(args.set, ddUdfIndexName);
    }

    /**
     * removes records where bin1 % 9 == 0; SC needs {@code withDurableDelete()} on the background job.
     */
    @Test
    public void backgroundUdfRemoveUsesWithDurableDeleteOnStrongConsistency() {
        Assumptions.assumeTrue(args.scMode,
            "Background UDF remove + durable delete is asserted for SC.");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature.");

        BackgroundUdfBuilder bg = session.backgroundTask()
            .executeUdf(args.set)
            .function("record_example", "processRecord")
            .passing(ddUdfBin1, ddUdfBin2, 100)
            .where("$." + ddUdfBin1 + " >= 3 and $." + ddUdfBin1 + " <= 9")
            .defaultWithDurableDelete();

        ExecuteTask task = bg.execute();
        task.waitTillComplete(3000, 3000);

        validateDdUdfProcessRecordOutcome();
    }

    private void validateDdUdfProcessRecordOutcome() {
        RecordStream rs = session.query(args.set)
            .where("$." + ddUdfBin1 + " >= 1 and $." + ddUdfBin1 + " <= " + (ddUdfSize + 100))
            .execute();

        try {
            int[] expectedList = new int[] { 1, 2, 3, 104, 5, 106, 7, 108, -1, 10 };
            int expectedSize = ddUdfSize - 1;
            int count = 0;

            while (rs.hasNext()) {
                Record rec = rs.next().recordOrThrow();
                int value1 = rec.getInt(ddUdfBin1);
                int value2 = rec.getInt(ddUdfBin2);

                if (value1 == 9) {
                    fail("Data mismatch. value1 9 should not exist after UDF remove");
                }

                if (value1 == 5) {
                    if (value2 != 0) {
                        fail("Data mismatch. value2 " + value2 + " should be null for bin2 cleared");
                    }
                }
                else if (value1 != expectedList[value2 - 1]) {
                    fail("Data mismatch. Expected " + expectedList[value2 - 1] + ". Received " + value1);
                }
                count++;
            }
            assertEquals(expectedSize, count);
        }
        finally {
            rs.close();
        }
    }

    /**
     * {@link com.aerospike.client.sdk.OperateTest} covers {@code upsert} + {@code deleteRecord()} + durable;
     * this covers {@code update} + {@code deleteRecord()} on SC.
     */
    @Test
    public void updateOperateDeleteRecordUsesWithDurableDeleteOnStrongConsistency() {
        Assumptions.assumeTrue(args.scMode,
            "Operate delete record + durable delete is asserted for SC.");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature.");

        Key key = args.set.id(10670);
        String binName = "udDelBin";

        session.insert(key).bin(binName).setTo(1).execute();

        RecordStream rs = session.update(key)
            .bin(binName).get()
            .deleteRecord()
            .defaultWithDurableDelete()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(1, rec.getInt(binName));

        rs = session.exists(key).execute();
        assertTrue(rs.hasNext());
        assertFalse(rs.next().asBoolean());
    }

    /**
     * Regression: {@link OperationSpec#getDurableDelete()} must be merged into batch delete wire format.
     * On SC without expunge, non-durable deletes can fail (or not reset state); without durable, a second
     * round of {@code add} would stack (e.g. 45 + 15 = 60). Two identical rounds must each yield 15.
     * <p>
     * Bin sums alone do not prove a <em>durable</em> tombstone vs expunge-like removal; see
     * {@link #durableDeleteTombstoneAdvancesGenerationAfterReinsert} for a generation-based check.
     */
    @Test
    public void batchDeleteDurableDeleteResetsRecordsForRepeatAdds() {
        Assumptions.assumeTrue(args.scMode,
            "Requires a strongly consistent namespace (durable batch delete vs repeat adds).");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature (see FilterExpTest#durableDelete).");

        String binName = "ddbatchbin";
        int firstKey = 10110;
        List<Key> keys = args.set.ids(
            firstKey, firstKey + 1, firstKey + 2, firstKey + 3, firstKey + 4,
            firstKey + 5, firstKey + 6, firstKey + 7, firstKey + 8, firstKey + 9);

        RecordStream del = session.delete(keys).withDurableDelete().execute();
        assertBatchDeleteStreamOk(del, keys.size());

        session.upsert(keys).bin(binName).add(10).execute();
        session.upsert(keys).bin(binName).add(5).execute();

        RecordStream rs = session.query(keys).readingOnlyBins(binName).execute();
        for (int i = 0; i < keys.size(); i++) {
            assertTrue(rs.hasNext(), "key index " + i);
            Record rec = rs.next().recordOrThrow();
            assertEquals(15, rec.getInt(binName), "key index " + i);
        }
    }

    /**
     * Durable delete leaves a tombstone, whereas aggressive expunge (or paths that erase prior lineage without that
     * write) can make a subsequent insert at the same user key look like a “fresh” record with a lower generation
     * counter.
     * After durable delete + insert, generation should be at least two steps above the
     * post-first-insert value (insert, then tombstone from durable delete, then insert again), not reset
     * as if the key had been wiped without the tombstone write.
     */
    @Test
    public void durableDeleteTombstoneAdvancesGenerationAfterReinsert() {
        Assumptions.assumeTrue(args.scMode,
            "Generation / tombstone semantics are asserted for strongly consistent namespaces.");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature.");

        Key key = args.set.id("ddg" + UUID.randomUUID().toString().replace("-", ""));
        String binName = "name";

        session.delete(key).withDurableDelete().execute();

        session.insert(key).bin(binName).setTo("bob").execute();
        int genAfterFirstInsert =
            session.query(key).readingOnlyBins(binName).execute().getFirstRecord().generation;

        session.delete(key).withDurableDelete().execute();
        session.insert(key).bin(binName).setTo("bob").execute();
        int genAfterSecondInsert =
            session.query(key).readingOnlyBins(binName).execute().getFirstRecord().generation;

        assertTrue(genAfterFirstInsert > 0, "generation after first insert must be positive");
        assertTrue(genAfterSecondInsert >= genAfterFirstInsert + 2,
            "durable delete + insert should advance generation (tombstone write), not reset like expunge-only; "
                + "after first insert gen=" + genAfterFirstInsert + ", after second insert gen=" + genAfterSecondInsert);
    }

    /**
     * Multi-key heterogeneous batch forces BatchWrite (not the single-key operate path).
     * withDurableDelete() must still reach the wire when {@link Settings#getUseDurableDelete()}
     * is false for non-retryable batch CP writes, otherwise SC may reject the delete or leave stale state
     * (repeat adds would not reset to 15).
     */
    @Test
    public void batchOperateRecordDeleteWithDurableDeleteOverridesBehaviorWhenMultiKey() {
        Assumptions.assumeTrue(args.scMode,
            "Requires a strongly consistent namespace (record delete inside operate).");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature.");

        Behavior probeBehavior = Behavior.DEFAULT.deriveWithChanges("BatchOperateDurableDeleteProbe", b -> b
            .on(Selectors.writes().nonRetryable().batch().cp(), ops -> ops.useDurableDelete(false)));
        ResolvedSettings batchWriteCp =
            probeBehavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.CP);
        assertFalse(batchWriteCp.getUseDurableDelete(),
            "probe behavior must keep batch CP non-retryable durable-delete off in Settings");

        Session probeSession = cluster.createSession(probeBehavior);

        // Bin names are limited to 15 characters on the server; keep short (was 20 chars → BIN_NAME_TOO_LONG).
        String binName = "ddOpDdBin";
        int firstKey = 10320;
        List<Key> keys = args.set.ids(firstKey, firstKey + 1, firstKey + 2, firstKey + 3);

        probeSession.upsert(keys).bin(binName).add(10).execute();
        probeSession.upsert(keys).bin(binName).add(5).execute();

        RecordStream del = probeSession.upsert(keys)
            .defaultWithDurableDelete()
            .bin(binName).get()
            .deleteRecord()
            .execute();
        assertBatchWriteOperateDeleteStreamAllOk(del, keys.size());

        probeSession.upsert(keys).bin(binName).add(10).execute();
        probeSession.upsert(keys).bin(binName).add(5).execute();

        RecordStream rs = probeSession.query(keys).readingOnlyBins(binName).execute();
        for (int i = 0; i < keys.size(); i++) {
            assertTrue(rs.hasNext(), "key index " + i);
            Record rec = rs.next().recordOrThrow();
            assertEquals(15, rec.getInt(binName), "key index " + i);
        }
    }

    /**
     * On SC, non-durable batch delete is typically forbidden ({@link ResultCode#FAIL_FORBIDDEN}).
     * Uses default {@link Session}; does not assert resolved {@link Settings#getUseDurableDelete()} is
     * true.
     */
    @Test
    public void batchDeleteExplicitNonDurableRejectedOnStrongConsistency() {
        Assumptions.assumeTrue(args.scMode,
            "Requires SC namespace policy that forbids non-durable deletes.");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete / SC delete policy is an Enterprise-relevant scenario.");

        String binName = "ddNdBin";
        int firstKey = 10430;
        List<Key> keys = args.set.ids(firstKey, firstKey + 1);

        session.upsert(keys).bin(binName).add(1).execute();

        RecordStream rs = session.delete(keys).withoutDurableDelete().execute();

        int count = 0;
        while (rs.hasNext()) {
            RecordResult rr = rs.next();
            assertEquals(ResultCode.FAIL_FORBIDDEN, rr.resultCode(),
                "expected non-durable batch delete to be forbidden on SC; got "
                    + rr.resultCode() + " (" + ResultCode.getResultString(rr.resultCode()) + ") key="
                    + rr.key());
            count++;
        }
        assertEquals(keys.size(), count);

        RecordStream exists = session.exists(keys).includeMissingKeys().execute();
        for (int i = 0; i < keys.size(); i++) {
            assertTrue(exists.hasNext(), "key index " + i);
            assertTrue(exists.next().asBoolean(), "record should still exist after forbidden delete; index " + i);
        }
    }

    /**
     * {@link ResolvedSettings#getUseDurableDelete()} is true for batch CP non-retryable writes, but
     * SC then rejects the delete ({@link ResultCode#FAIL_FORBIDDEN}).
     */
    @Test
    public void batchDeleteExplicitNonDurableRejectedWhenBehaviorDurableTrue() {
        Assumptions.assumeTrue(args.scMode,
            "Requires SC namespace policy that forbids non-durable deletes.");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete / SC delete policy is an Enterprise-relevant scenario.");

        Behavior probeBehavior = Behavior.DEFAULT.deriveWithChanges("BatchDdFalseOvProbe", b -> b
            .on(Selectors.writes().nonRetryable().batch().cp(), ops -> ops.useDurableDelete(true)));
        ResolvedSettings batchWriteCp =
            probeBehavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.CP);
        assertTrue(batchWriteCp.getUseDurableDelete(),
            "probe: batch CP non-retryable durable-delete must be on in Settings (override false must win)");

        Session probeSession = cluster.createSession(probeBehavior);

        String binName = "ddFbBin";
        int firstKey = 10460;
        List<Key> keys = args.set.ids(firstKey, firstKey + 1);

        probeSession.upsert(keys).bin(binName).add(1).execute();

        RecordStream rs = probeSession.delete(keys).withoutDurableDelete().execute();

        int count = 0;
        while (rs.hasNext()) {
            RecordResult rr = rs.next();
            assertEquals(ResultCode.FAIL_FORBIDDEN, rr.resultCode(),
                "settings true + durableDelete(false) must not send durable delete on SC; got "
                    + rr.resultCode() + " (" + ResultCode.getResultString(rr.resultCode()) + ") key="
                    + rr.key());
            count++;
        }
        assertEquals(keys.size(), count);

        RecordStream exists = probeSession.exists(keys).includeMissingKeys().execute();
        for (int i = 0; i < keys.size(); i++) {
            assertTrue(exists.hasNext(), "key index " + i);
            assertTrue(exists.next().asBoolean(), "record should still exist after forbidden delete; index " + i);
        }
    }

    /**
     * Batch {@link Settings#getUseDurableDelete()} is false (probe behavior), but
     * {@code withDurableDelete()} must still send durable delete: SC accepts the delete and repeat adds
     * reset to 15.
     */
    @Test
    public void batchDeleteDurableOverrideTrueWhenBehaviorBatchDurableDeleteFalse() {
        Assumptions.assumeTrue(args.scMode,
            "Requires a strongly consistent namespace (durable batch delete vs repeat adds).");
        Assumptions.assumeTrue(args.enterprise,
            "Durable delete is an Enterprise server feature.");

        Behavior probeBehavior = Behavior.DEFAULT.deriveWithChanges("BatchDdTrueOvProbe", b -> b
            .on(Selectors.writes().nonRetryable().batch().cp(), ops -> ops.useDurableDelete(false)));
        ResolvedSettings batchWriteCp =
            probeBehavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.CP);
        assertFalse(batchWriteCp.getUseDurableDelete(),
            "probe: batch CP non-retryable durable-delete must be off in Settings (override supplies true)");

        Session probeSession = cluster.createSession(probeBehavior);

        String binName = "ddOvBin";
        int firstKey = 10450;
        List<Key> keys = args.set.ids(firstKey, firstKey + 1, firstKey + 2, firstKey + 3);

        probeSession.upsert(keys).bin(binName).add(10).execute();
        probeSession.upsert(keys).bin(binName).add(5).execute();

        RecordStream del = probeSession.delete(keys).withDurableDelete().execute();
        assertBatchDeleteStreamOk(del, keys.size());

        probeSession.upsert(keys).bin(binName).add(10).execute();
        probeSession.upsert(keys).bin(binName).add(5).execute();

        RecordStream rs = probeSession.query(keys).readingOnlyBins(binName).execute();
        for (int i = 0; i < keys.size(); i++) {
            assertTrue(rs.hasNext(), "key index " + i);
            Record rec = rs.next().recordOrThrow();
            assertEquals(15, rec.getInt(binName), "key index " + i);
        }
    }

    @Test
    public void defaultSessionPointDeleteOnStrongConsistencyWithoutExplicitDurableOptIn() {
        Assumptions.assumeTrue(args.scMode,
            "Strong-consistency namespace only.");

        Key key = args.set.id(10710);
        String binName = "ddbDefSc";
        session.upsert(key).bin(binName).setTo(1).execute();

        RecordStream del = session.delete(key).execute();
        assertTrue(del.hasNext());
        assertTrue(del.next().asBoolean(),
            "DEFAULT Behavior on SC: point delete without fluent durable opt-in should still succeed");

        RecordStream ex = session.exists(key).execute();
        assertTrue(ex.hasNext());
        assertFalse(ex.next().asBoolean());
    }

    /**
     * Baseline on AP: {@link Session} uses {@link Behavior#DEFAULT} and the caller does not set durable
     * explicitly; point delete removes the record.
     */
    @Test
    public void defaultSessionPointDeleteOnApWithoutExplicitDurableOptIn() {
        Assumptions.assumeFalse(args.scMode,
            "AP namespace only.");

        Key key = args.set.id(10711);
        String binName = "ddbDefAp";
        session.upsert(key).bin(binName).setTo(1).execute();

        RecordStream del = session.delete(key).execute();
        assertTrue(del.hasNext());
        assertTrue(del.next().asBoolean());

        RecordStream ex = session.exists(key).execute();
        assertTrue(ex.hasNext());
        assertFalse(ex.next().asBoolean());
    }

    private static void assertBatchDeleteStreamOk(RecordStream stream, int expectedCount) {
        int count = 0;
        while (stream.hasNext()) {
            RecordResult rr = stream.next();
            int rc = rr.resultCode();
            assertTrue(rc == ResultCode.OK || rc == ResultCode.KEY_NOT_FOUND_ERROR,
                "unexpected delete resultCode=" + rc + " key=" + rr.key());
            count++;
        }
        assertEquals(expectedCount, count);
    }

    private static void assertBatchWriteOperateDeleteStreamAllOk(RecordStream stream, int expectedCount) {
        int count = 0;
        while (stream.hasNext()) {
            RecordResult rr = stream.next();
            int rc = rr.resultCode();
            assertEquals(ResultCode.OK, rc,
                "unexpected operate-delete resultCode=" + rc + " (" + ResultCode.getResultString(rc) + ") key="
                    + rr.key());
            count++;
        }
        assertEquals(expectedCount, count);
    }
}
