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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class BinBuilderSetToObjectTest extends ClusterTest {

    @Test
    public void setToObjectInfersWireType() {
        Key key = args.set.id("setToObject");
        session.delete(key).execute();

        Object stringVal = "hello";
        Object intVal = 42;
        Object longVal = 99L;
        Object boolVal = true;
        Object doubleVal = 1.5d;
        Object blobVal = new byte[] {1, 2, 3};
        Object listVal = List.of("a", "b");
        Object mapVal = Map.of("k", "v");

        session.upsert(key)
            .bin("s").setTo(stringVal)
            .bin("i").setTo(intVal)
            .bin("l").setTo(longVal)
            .bin("b").setTo(boolVal)
            .bin("d").setTo(doubleVal)
            .bin("blob").setTo(blobVal)
            .bin("list").setTo(listVal)
            .bin("map").setTo(mapVal)
            .bin("gone").setTo("drop-me")
            .execute();

        Object nil = null;
        session.upsert(key).bin("gone").setTo(nil).execute();

        Record rec = session.query(key).execute().getFirstRecord();
        assertEquals("hello", rec.getString("s"));
        assertEquals(42L, rec.getLong("i"));
        assertEquals(99L, rec.getLong("l"));
        assertEquals(true, rec.getBoolean("b"));
        assertEquals(1.5d, rec.getDouble("d"));
        assertArrayEquals(new byte[] {1, 2, 3}, rec.getBytes("blob"));
        assertEquals(List.of("a", "b"), rec.getList("list"));
        assertEquals(Map.of("k", "v"), rec.getMap("map"));
        assertNull(rec.getValue("gone"));
    }

    @Test
    public void setToObjectRejectsUnsupportedType() {
        Key key = args.set.id("setToObjectBad");
        Object unsupported = new StringBuilder("nope");
        assertThrows(AerospikeException.class,
            () -> session.upsert(key).bin("x").setTo(unsupported));
    }
}
