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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.policy.RecordExistsAction;

public class ReplaceTest extends ClusterTest {
    @Test
    public void replace() {
        Key key = args.set.id("replacekey");

        session.upsert(key)
                .bins("bin1", "bin2")
                .values("value1", "value2")
                .execute();

        session.upsert(key)
                .withRecordExistsAction(RecordExistsAction.REPLACE)
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
}
