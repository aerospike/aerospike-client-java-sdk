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
package com.aerospike.examples.fixtures;

import java.util.List;
import java.util.Objects;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;

public final class ExampleAssertions {
    private ExampleAssertions() {
    }

    public static void truncate(Session session, DataSet dataSet) {
        session.truncate(dataSet);
    }

    public static long count(Session session, DataSet dataSet) {
        long count = 0;

        try (RecordStream stream = session.query(dataSet).execute()) {
            while (stream.hasNext()) {
                stream.next().orThrow();
                count++;
            }
        }

        return count;
    }

    public static void assertCount(Session session, DataSet dataSet, long expected) {
        long actual = count(session, dataSet);

        if (actual != expected) {
            throw new AssertionError("Expected " + expected + " records in " + dataSet + " but found " + actual);
        }
    }

    public static Record assertRecordExists(Session session, DataSet dataSet, Object id) {
        try (RecordStream stream = session.query(dataSet.ids(List.of(id))).execute()) {
            if (!stream.hasNext()) {
                throw new AssertionError("Expected record id " + id + " in " + dataSet + " but no result was returned");
            }

            RecordResult result = stream.next().orThrow();
            Record record = result.recordOrThrow();

            if (record == null) {
                throw new AssertionError("Expected record id " + id + " in " + dataSet + " but record payload was null");
            }

            return record;
        }
    }

    public static void assertRecordMissing(Session session, DataSet dataSet, Object id) {
        try (RecordStream stream = session.query(dataSet.ids(List.of(id))).execute()) {
            if (!stream.hasNext()) {
                return;
            }

            RecordResult result = stream.next();

            if (result.isOk()) {
                throw new AssertionError("Expected record id " + id + " in " + dataSet + " to be missing");
            }
        }
    }

    public static void assertBinEquals(Session session, DataSet dataSet, Object id, String binName, Object expected) {
        Record record = assertRecordExists(session, dataSet, id);
        Object actual = record.getValue(binName);

        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(
                "Expected " + dataSet + " id " + id + " bin " + binName + " to be " + expected + " but found " + actual);
        }
    }
}
