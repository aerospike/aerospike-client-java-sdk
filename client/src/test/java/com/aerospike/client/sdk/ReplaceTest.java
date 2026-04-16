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
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

public class ReplaceTest extends ClusterTest {
    @Test
    public void replace() {
        Key key = args.set.id("replacekey");

        session.upsert(key)
                .bins("bin1", "bin2")
                .values("value1", "value2")
                .execute();

        session.replace(key)
                .bins("bin3")
                .values("value3")
                .execute();

        RecordStream rs = session.query(key).execute();
        Record record = rs.next().recordOrThrow();
        assertNotNull(record);

        assertNull(record.getValue("bin1"));
        assertNull(record.getValue("bin2"));
        assertEquals("value3", record.getValue("bin3"));
    }

    @Test
    public void replaceOnly() {
        Key key = args.set.id("replaceonlykey");

        session.delete(key).execute();

        try {
            RecordStream rs = session.upsert(key)
                    .replaceOnly()
                    .bins("bin")
                    .values("value")
                    .execute();

            RecordResult rr = rs.next();
            rr.orThrow();

            fail("Failure. This command should have resulted in an error.");
        } catch (AerospikeException ae) {
            assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, ae.getResultCode());
        }
    }

    @Test
    public void replaceOnlyModifiesOpType() {
        Key key = args.set.id("replaceonlymodifieskey");

        session.upsert(key)
                .bins("bin1", "bin2")
                .values("value1", "value2")
                .execute();

        session.upsert(key)
                .replaceOnly()
                .bins("bin3")
                .values("value3")
                .execute();

        RecordStream rs = session.query(key).execute();
        Record record = rs.next().recordOrThrow();
        assertNotNull(record);
        assertNull(record.getValue("bin1"));
        assertNull(record.getValue("bin2"));
        assertEquals("value3", record.getValue("bin3"));
    }

    @Test
    public void chainedOperationsWithDifferentOpTypes() {
        Key key1 = args.set.id("chainkey1");
        Key key2 = args.set.id("chainkey2");
        Key key3 = args.set.id("chainkey3");

        session.upsert(key1).bins("value").values("original1").execute();
        session.upsert(key2).bins("value").values("original2").execute();
        session.delete(key3).execute();

        session.update(key1)
                .bin("value").setTo("updated1")
            .insert(key2)
                .bin("newbin").setTo("inserted2")
            .replace(key3)
                .bin("value").setTo("replaced3")
            .execute();

        Record rec1 = session.query(key1).execute().next().recordOrThrow();
        assertEquals("updated1", rec1.getValue("value"));

        Record rec2 = session.query(key2).execute().next().recordOrThrow();
        assertEquals("original2", rec2.getValue("value"));
        assertNull(rec2.getValue("newbin"));

        Record rec3 = session.query(key3).execute().next().recordOrThrow();
        assertEquals("replaced3", rec3.getValue("value"));
    }
}
