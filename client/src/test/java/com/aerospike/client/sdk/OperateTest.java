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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

public class OperateTest extends ClusterTest {
    /** SC requires durable delete for record deletes inside operate; keep opt-in on AP. */
    private ChainableOperationBuilder upsertForScDurableRecordDelete(Key key) {
        ChainableOperationBuilder b = session.upsert(key);
        return args.scMode ? b.withDurableDelete() : b;
    }

    @Test
    public void operate() {
        // Write initial record.
        Key key = args.set.id("operate");
        String binName1 = "optintbin";
        String binName2 = "optstringbin";

        session.upsert(key)
            .bin(binName1).setTo(7)
            .bin(binName2).setTo("string value")
            .execute();

        // Add integer, write new string and read record.
        RecordStream rs = session.upsert(key)
            .bin(binName1).add(4)
            .bin(binName2).setTo("new string")
            .bin(binName1).get()
            .bin(binName2).get()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        List<?> list1 = rec.getList(binName1);
        List<?> list2 = rec.getList(binName2);

        assertEquals(11, (int)(long)list1.get(1));
        assertEquals("new string", list2.get(1));
    }

    @Test
    public void invalidOperationTypeMismatchReturnsError() {
        Key key = args.set.id("invalidOp");

        // Write an integer bin.
        session.upsert(key)
            .bin("intbin").setTo(42)
            .execute();

        // Attempt a list append on an integer bin (type mismatch).
        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            RecordStream rs = session.upsert(key)
                .bin("intbin").listAppend("item")
                .execute();

            assertTrue(rs.hasNext());
            rs.next().recordOrThrow();
        });

        //Apr 10 2026 07:37:45 GMT: WARNING (particle) cdt_process_state_packed_list_modify_optype() invalid type 1
        int rc = ae.getResultCode();
        assertEquals(ResultCode.BIN_TYPE_ERROR, rc, "Expected BIN_TYPE_ERROR, got: " +
                rc + " (" + ResultCode.getResultString(rc) + ")");
    }

    @Test
    public void operateDeleteRecord() {
        Key key = args.set.id("operateDelete");
        String binName1 = "optintbin1";
        String binName2 = "optintbin2";

        // Write initial record.
        session.upsert(key)
            .bin(binName1).setTo(1)
            .execute();

        // Read bin1 and then delete the record atomically.
        RecordStream rs = upsertForScDurableRecordDelete(key)
            .bin(binName1).get()
            .deleteRecord()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(1, rec.getLong(binName1));

        // Verify record is gone.
        rs = session.exists(key).execute();
        assertTrue(rs.hasNext());
        assertFalse(rs.next().asBoolean());

        // Rewrite record.
        session.insert(key)
            .bin(binName1).setTo(1)
            .bin(binName2).setTo(2)
            .execute();

        // Read bin 1 and then delete all followed by a write of bin2.
        rs = upsertForScDurableRecordDelete(key)
            .bin(binName1).get()
            .deleteRecord()
            .bin(binName2).setTo(2)
            .bin(binName2).get()
            .execute();

        rec = rs.getFirstRecord();
        assertEquals(1, rec.getInt(binName1));

        // Read record.
        rec = session.query(key).execute().getFirstRecord();
        assertEquals(2, rec.getInt(binName2));
        assertTrue(rec.getValue(binName1) == null);
        assertEquals(1, rec.bins.size());
    }

    @Test
    public void operateTouchRecord() {
        assumeTrue(args.hasTtl, "Server/namespace does not support TTL writes; configure nsup-period or allow-ttl-without-nsup");

        Key key = args.set.id("operateTouch");
        String binName1 = "optintbin1";

        // Write initial record with a 60-second TTL.
        session.upsert(key)
            .bin(binName1).setTo(42)
            .expireRecordAfterSeconds(60)
            .execute();

        // Read bin1 and touch the record to reset TTL atomically.
        RecordStream rs = session.upsert(key)
            .bin(binName1).get()
            .touchRecord()
            .expireRecordAfterSeconds(120)
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(42, rec.getLong(binName1));
    }

    @Test
    public void operateDeleteRecordWithRewrite() {
        Key key = args.set.id("operateDeleteRewrite");
        String binName1 = "optintbin1";
        String binName2 = "optintbin2";

        // Write initial record.
        session.upsert(key)
            .bin(binName1).setTo(1)
            .bin(binName2).setTo(2)
            .execute();

        // Read bin1, delete record, rewrite bin2 -- all atomically.
        RecordStream rs = upsertForScDurableRecordDelete(key)
            .bin(binName1).get()
            .deleteRecord()
            .bin(binName2).setTo(99)
            .bin(binName2).get()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(1, rec.getLong(binName1));
        List<?> list2 = rec.getList(binName2);
        assertEquals(99, (int)(long)list2.get(1));

        // Verify only bin2 exists (bin1 was deleted with the record, then bin2 was rewritten).
        rs = session.query(key).execute();
        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        assertEquals(99, rec.getLong(binName2));
        assertTrue(rec.getValue(binName1) == null);
    }
}
