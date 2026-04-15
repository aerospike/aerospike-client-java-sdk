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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

public class TouchTest extends ClusterTest {
    @Test
    public void touchOperate() {
        assumeTrue(args.hasTtl, "Server does not support TTL");

        Key key = args.set.id("touchOperate");

        session.upsert(key)
                .bins("touchbin")
                .values("touchvalue")
                .expireRecordAfter(Duration.ofSeconds(1))
                .execute();

        session.touch(key)
                .expireRecordAfter(Duration.ofSeconds(2))
                .execute();

        RecordStream rs = session.query(key).withNoBins().execute();
        Record record = rs.next().recordOrThrow();
        assertNotNull(record);
        assertNotEquals(0, record.expiration);

        sleep(1000);

        rs = session.query(key).readingOnlyBins("touchbin").execute();
        record = rs.next().recordOrNull();
        assertNotNull(record);

        sleep(3000);

        rs = session.query(key).readingOnlyBins("touchbin").execute();
        RecordResult rr = rs.next();
        assertNull(rr);
    }

    @Test
    public void touch() {
        assumeTrue(args.hasTtl, "Server does not support TTL");

        Key key = args.set.id("touch");

        session.upsert(key)
                .bins("touchbin")
                .values("touchvalue")
                .expireRecordAfter(Duration.ofSeconds(1))
                .execute();

        session.touch(key)
                .expireRecordAfter(Duration.ofSeconds(2))
                .execute();

        sleep(1000);

        RecordStream rs = session.query(key).withNoBins().execute();
        Record record = rs.next().recordOrNull();
        assertNotNull(record);
        assertNotEquals(0, record.expiration);

        sleep(3000);

        rs = session.query(key).withNoBins().execute();
        RecordResult rr = rs.next();
        assertNull(rr);
    }

    @Test
    public void touched() {
        assumeTrue(args.hasTtl, "Server does not support TTL");

        Key key = args.set.id("touched");

        session.delete(key).execute();

        RecordStream rs = session.touch(key)
                .expireRecordAfter(Duration.ofSeconds(10))
                .execute();
        RecordResult rr = rs.next();
        assertFalse(rr.asBoolean());

        session.upsert(key)
                .bins("touchbin")
                .values("touchvalue")
                .expireRecordAfter(Duration.ofSeconds(10))
                .execute();

        rs = session.touch(key)
                .expireRecordAfter(Duration.ofSeconds(10))
                .execute();
        rr = rs.next();
        assertTrue(rr.asBoolean());
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
