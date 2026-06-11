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

import java.util.Arrays;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.util.Version;

/**
 * CDT list increments pinned for <strong>Aerospike server 8+</strong> (floor toward
 * {@code -Infinity} when the list cell stores an integer and the increment is
 * floating-point — same sequence as the internal QE / John Traver trace).
 * <p>
 * Pairs with {@code test_cdt_list_server8_john_sequence.py} in aerospike-client-python.
 * <ol>
 *   <li>{@link #javaWritesJohnListForPythonReader} — then Python
 *       {@code test_python_reads_java_written_list}</li>
 *   <li>Python {@code test_python_writes_john_list_for_java_reader} — then
 *       {@link #javaReadsPythonJohnList_ifPresent}</li>
 * </ol>
 */
public class CdtListServer8JohnSequenceTest extends ClusterTest {

    private static final String DEMO_SET = "demo";
    private static final String BIN = "list1";
    private static final String KEY_JAVA = "crossCdtListJohnJavaWrote";
    private static final String KEY_PYTHON = "crossCdtListJohnPythonWrote";

    private Key demoKey(String userKey) {
        return DataSet.of(args.namespace, DEMO_SET).id(userKey);
    }

    private void assumeServer8Plus() {
        Version v = cluster.getRandomNode().getVersion();
        Assumptions.assumeTrue(v.isGreaterOrEqual(8, 0, 0, 0),
            () -> "Pinned for server 8+; this node reports " + v);
    }

    private static void assertList24And0(AerospikeList<?> lst) {
        assertNotNull(lst);
        assertEquals(2, lst.size());
        assertEquals(24L, ((Number) lst.get(0)).longValue());
        assertEquals(0L, ((Number) lst.get(1)).longValue());
    }

    private void runJohnIncrements(Key key) {
        session.delete(key).execute();

        session.upsert(key)
            .bin(BIN).listAppendItems(Arrays.asList(0, 0))
            .bin(BIN).listIncrement(0, 1L)
            .bin(BIN).listIncrement(0, 2.5)
            .bin(BIN).listIncrement(0, 6.25)
            .bin(BIN).listIncrement(0, 15.625)
            .execute();

        Record rec = session.query(key)
            .readingOnlyBins(BIN)
            .execute()
            .getFirstRecord();

        assertList24And0(rec.getList(BIN));
    }

    @Test
    public void johnTraverFloatIncrementsOnIntCells_server8Pinned() {
        assumeServer8Plus();
        runJohnIncrements(demoKey("johnSeqJavaOnly"));
    }

    @Test
    public void javaWritesJohnListForPythonReader() {
        assumeServer8Plus();
        runJohnIncrements(demoKey(KEY_JAVA));
    }

    @Test
    public void javaReadsPythonJohnList_ifPresent() {
        assumeServer8Plus();
        Key key = demoKey(KEY_PYTHON);
        Record rec = session.query(key)
            .readingOnlyBins(BIN)
            .execute()
            .getFirstRecord();
        Assumptions.assumeTrue(rec != null,
            () -> "Run test_cdt_list_server8_john_sequence.test_python_writes_john_list_for_java_reader first.");

        assertList24And0(rec.getList(BIN));
    }
}
