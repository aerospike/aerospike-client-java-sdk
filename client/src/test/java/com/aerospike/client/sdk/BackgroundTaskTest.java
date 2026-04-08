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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.task.ExecuteTask;
import com.aerospike.client.sdk.task.RegisterTask;

public class BackgroundTaskTest extends ClusterTest {

    private static final String BG_BIN = "bgval";
    private static final String BG_BIN2 = "bgval2";

    private static DataSet bgSet;

    @BeforeAll
    public static void setup() {
        bgSet = DataSet.of(args.namespace, "bgtest");

        // Register UDF for background UDF tests
        String lua = """
            local function putBin(r, name, value)
                if not aerospike:exists(r) then aerospike:create(r) end
                r[name] = value
                aerospike:update(r)
            end

            function writeBin(r, name, value)
                putBin(r, name, value)
            end

            function incrementBin(r, name, amount)
                if not aerospike:exists(r) then
                    aerospike:create(r)
                    r[name] = 0
                end
                r[name] = r[name] + amount
                aerospike:update(r)
            end

            function writeWithValidation(r, name, value)
                if (value >= 1 and value <= 10) then
                    putBin(r, name, value)
                else
                    error("1000:Invalid value")
                end
            end
            """;
        RegisterTask task = session.registerUdfString(lua, "bg_test_example.lua");
        task.waitTillComplete();
    }

    @BeforeEach
    public void seedData() {
        for (int i = 1; i <= 10; i++) {
            session.upsert(bgSet.id("bg_" + i))
                .bin(BG_BIN).setTo(i)
                .bin(BG_BIN2).setTo("original")
                .execute();
        }
    }

    // ========================================
    // Background Update Tests
    // ========================================

    @Test
    public void backgroundUpdate() {
        ExecuteTask task = session.backgroundTask()
            .update(bgSet)
            .bin(BG_BIN2).setTo("updated")
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .readingOnlyBins(BG_BIN2)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("updated", rec.getString(BG_BIN2));
        }
    }

    @Test
    public void backgroundUpdateWithWhere() {
        ExecuteTask task = session.backgroundTask()
            .update(bgSet)
            .where("$.bgval > 5")
            .bin(BG_BIN2).setTo("filtered")
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .readingOnlyBins(BG_BIN2)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            if (i > 5) {
                assertEquals("filtered", rec.getString(BG_BIN2));
            } else {
                assertEquals("original", rec.getString(BG_BIN2));
            }
        }
    }

    // ========================================
    // Background Delete Tests
    // ========================================

    @Test
    public void backgroundDelete() {
        ExecuteTask task = session.backgroundTask()
            .delete(bgSet)
            .where("$.bgval > 8")
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .execute();

            RecordResult rr = rs.next();
            if (i > 8) {
                assertNull(rr);
            } else {
                assertNotNull(rr);
                assertNotNull(rr.recordOrThrow());
            }
        }
    }

    // ========================================
    // Background Touch Tests
    // ========================================

    @Test
    public void backgroundTouch() {
        assumeTrue(args.hasTtl, "Server does not support TTL");

        ExecuteTask task = session.backgroundTask()
            .touch(bgSet)
            .expireRecordAfter(Duration.ofSeconds(60))
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertTrue(rec.expiration > 0, "Record should have a non-zero TTL after touch");
        }
    }

    // ========================================
    // Background UDF Tests
    // ========================================

    @Test
    public void backgroundUdf() {
        ExecuteTask task = session.backgroundTask()
            .executeUdf(bgSet)
            .function("bg_test_example", "writeBin")
            .passing(BG_BIN2, "udf_written")
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .readingOnlyBins(BG_BIN2)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals("udf_written", rec.getString(BG_BIN2));
        }
    }

    @Test
    public void backgroundUdfWithArgs() {
        ExecuteTask task = session.backgroundTask()
            .executeUdf(bgSet)
            .function("bg_test_example", "incrementBin")
            .passing(BG_BIN, 100)
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .readingOnlyBins(BG_BIN)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            assertEquals(i + 100, rec.getInt(BG_BIN));
        }
    }

    @Test
    public void backgroundUdfWithWhere() {
        ExecuteTask task = session.backgroundTask()
            .executeUdf(bgSet)
            .function("bg_test_example", "writeBin")
            .passing(BG_BIN2, "udf_filtered")
            .where("$.bgval <= 3")
            .execute();

        task.waitTillComplete();

        for (int i = 1; i <= 10; i++) {
            RecordStream rs = session.query(bgSet.id("bg_" + i))
                .readingOnlyBins(BG_BIN2)
                .execute();

            assertTrue(rs.hasNext());
            Record rec = rs.next().recordOrThrow();
            if (i <= 3) {
                assertEquals("udf_filtered", rec.getString(BG_BIN2));
            } else {
                assertEquals("original", rec.getString(BG_BIN2));
            }
        }
    }

    @Test
    public void backgroundUdfWithRecordsPerSecond() {
        ExecuteTask task = session.backgroundTask()
            .executeUdf(bgSet)
            .function("bg_test_example", "writeBin")
            .passing(BG_BIN2, "rate_limited")
            .recordsPerSecond(100)
            .execute();

        task.waitTillComplete();

        RecordStream rs = session.query(bgSet.id("bg_1"))
            .readingOnlyBins(BG_BIN2)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals("rate_limited", rec.getString(BG_BIN2));
    }

    @Test
    public void backgroundUdfNoArgs() {
        ExecuteTask task = session.backgroundTask()
            .executeUdf(bgSet)
            .function("bg_test_example", "writeBin")
            .passing(BG_BIN2, "no_extra_args")
            .execute();

        task.waitTillComplete();

        RecordStream rs = session.query(bgSet.id("bg_1"))
            .readingOnlyBins(BG_BIN2)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals("no_extra_args", rec.getString(BG_BIN2));
    }

    // ========================================
    // Validation Tests
    // ========================================

    @Test
    public void backgroundUdfRejectsNullPackageName() {
        assertThrows(IllegalArgumentException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function(null, "writeBin")
        );
    }

    @Test
    public void backgroundUdfRejectsEmptyPackageName() {
        assertThrows(IllegalArgumentException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function("", "writeBin")
        );
    }

    @Test
    public void backgroundUdfRejectsNullFunctionName() {
        assertThrows(IllegalArgumentException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function("bg_test_example", null)
        );
    }

    @Test
    public void backgroundUdfRejectsEmptyFunctionName() {
        assertThrows(IllegalArgumentException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function("bg_test_example", "")
        );
    }

    @Test
    public void backgroundUpdateRejectsInvalidOpType() {
        assertThrows(IllegalArgumentException.class, () ->
            new BackgroundOperationBuilder(session, bgSet, OpType.UPSERT)
        );
    }

    @Test
    public void backgroundUdfFailOnFilteredOutThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function("bg_test_example", "writeBin")
                .failOnFilteredOut()
        );
    }

    @Test
    public void backgroundUdfRespondAllKeysThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            session.backgroundTask()
                .executeUdf(bgSet)
                .function("bg_test_example", "writeBin")
                .includeMissingKeys()
        );
    }

    @Test
    public void backgroundOperationFailOnFilteredOutThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            session.backgroundTask()
                .update(bgSet)
                .failOnFilteredOut()
        );
    }

    @Test
    public void backgroundOperationRespondAllKeysThrows() {
        assertThrows(UnsupportedOperationException.class, () ->
            session.backgroundTask()
                .update(bgSet)
                .includeMissingKeys()
        );
    }
}
