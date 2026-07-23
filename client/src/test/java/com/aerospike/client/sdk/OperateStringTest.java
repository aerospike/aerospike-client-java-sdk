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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.StringExp;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.util.Version;

/**
 * Integration tests for server string ops (8.1.3+): {@link com.aerospike.client.sdk.BinBuilder},
 * {@link ChainableOperationBuilder#appendOperations} with {@link StringOperation}, and
 * {@code selectFrom} with {@link StringExp}.
 */
public class OperateStringTest extends ClusterTest {

    @Test
    public void binBuilderStringReads() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "String ops require server 8.1.3+");

        Key key = args.set.id("stringBinFluentReads");
        session.upsert(key)
            .bin("s").setTo("hello")
            .execute();

        RecordStream rs = session.upsert(key)
            .bin("s").strlen()
            .bin("s").substr(1, 4)
            .bin("s").find("ll")
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(5L, rec.operationResult(0).getLong());
        assertEquals("ell", rec.operationResult(1).getString());
        assertEquals(2L, rec.operationResult(2).getLong());
    }

    @Test
    public void binBuilderStringModifyAndRead() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "String ops require server 8.1.3+");

        Key key = args.set.id("stringBinFluentModify");
        session.upsert(key)
            .bin("s").setTo("ab")
            .execute();

        RecordStream rs = session.upsert(key)
            .bin("s").upper()
            .bin("s").get()
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals("AB", rec.operationResult(0).getString());
        assertEquals("AB", rec.operationResult(1).getString());
    }

    @Test
    public void stringReadsViaAppendOperations() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "String ops require server 8.1.3+");

        Key key = args.set.id("stringAppendOperations");
        session.upsert(key)
            .bin("s").setTo("hello")
            .execute();

        RecordStream rs = session.upsert(key)
            .appendOperations(
                StringOperation.strlen("s"),
                StringOperation.substr("s", 1, 4),
                StringOperation.substr("s", 3),
                StringOperation.find("s", "ll"))
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(5L, rec.operationResult(0).getLong());
        assertEquals("ell", rec.operationResult(1).getString());
        assertEquals("lo", rec.operationResult(2).getString());
        assertEquals(2L, rec.operationResult(3).getLong());
    }

    @Test
    public void stringProjectionViaStringExpOnQuery() {
        Assumptions.assumeTrue(args.serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1_3),
            "String ops require server 8.1.3+");

        Key key = args.set.id("stringExpQuery");
        session.upsert(key)
            .bin("s").setTo("hello")
            .execute();

        RecordStream rs = session.query(key)
            .bin("slen").selectFrom(StringExp.strlen(Exp.stringBin("s")))
            .bin("stail").selectFrom(StringExp.substr(Exp.val(3), Exp.stringBin("s")))
            .bin("sfind").selectFrom(StringExp.find(Exp.val("ll"), Exp.stringBin("s")))
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        assertEquals(5L, rec.getLong("slen"));
        assertEquals("lo", rec.getString("stail"));
        assertEquals(2L, rec.getLong("sfind"));
    }
}
