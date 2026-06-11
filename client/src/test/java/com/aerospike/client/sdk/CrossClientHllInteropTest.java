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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.aerospike.client.sdk.Value.HLLValue;

/**
 * Cross-language HLL interoperability with the Aerospike Python client
 * ({@code test_cross_client_hll_interop.py}): same namespace, set, user key, bin
 * names, and {@code hll_add} parameters so the server stores one HLL value.
 * <p>
 * Run order for full coverage (same cluster):
 * <ol>
 *   <li>{@link #javaWritesHllWithWireProofForPythonReader} — creates the Java-owned record.</li>
 *   <li>Python {@code test_python_reads_java_hll_same_wire_bytes} — verifies typed read + wire.</li>
 *   <li>Python {@code test_python_writes_hll_with_wire_proof_for_java_reader} — creates the Python-owned record.</li>
 *   <li>{@link #javaReadsHllWrittenByPythonSameWireBytes} — verifies typed read + wire (aborts if step 3 not run).</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrossClientHllInteropTest extends ClusterTest {

    /** Match {@code test_cross_client_hll_interop.py} (namespace from {@link Args}). */
    private static final String DEMO_SET = "demo";
    private static final String KEY_JAVA = "crossHllInteropJavaWrote";
    private static final String KEY_PYTHON = "crossHllInteropPythonWrote";
    private static final String BIN = "hllbin";
    /** Base64 of raw HLL register bytes; used to prove wire equality across clients. */
    private static final String WIRE_B64_BIN = "wire_b64";

    private static final List<String> ENTRIES = Arrays.asList("aa", "bb", "cc");

    private Key demoKey(String userKey) {
        return DataSet.of(args.namespace, DEMO_SET).id(userKey);
    }

    @Test
    @Order(1)
    public void javaWritesHllWithWireProofForPythonReader() {
        Key key = demoKey(KEY_JAVA);
        session.delete(key).execute();

        session.upsert(key)
            .bin(BIN).hllAdd(new ArrayList<>(ENTRIES), HllConfig.of(8))
            .execute();

        Record rec = session.upsert(key)
            .bin(BIN).get()
            .execute()
            .getFirstRecord();

        // Single op on this bin: value is HLLValue directly, not AerospikeList
        // (multiple ops on the same bin pack per-op results into a list).
        HLLValue hll = rec.getHLLValue(BIN);
        assertNotNull(hll);
        byte[] wire = hll.getBytes();
        String b64 = Base64.getEncoder().encodeToString(wire);

        session.upsert(key)
            .bin(WIRE_B64_BIN).setTo(b64)
            .execute();

        Record q = session.query(key)
            .readingOnlyBins(BIN, WIRE_B64_BIN)
            .execute()
            .getFirstRecord();

        assertNotNull(q.getHLLValue(BIN));
        assertArrayEquals(wire, Base64.getDecoder().decode(q.getString(WIRE_B64_BIN)));
    }

    @Test
    @Order(2)
    public void javaReadsHllWrittenByPythonSameWireBytes() {
        Key key = demoKey(KEY_PYTHON);
        Record rec = session.query(key)
            .readingOnlyBins(BIN, WIRE_B64_BIN)
            .execute()
            .getFirstRecord();
        Assumptions.assumeTrue(rec != null,
            () -> "Run Python test_cross_client_hll_interop.test_python_writes_hll_with_wire_proof_for_java_reader first");

        HLLValue hll = rec.getHLLValue(BIN);
        assertNotNull(hll, "Python-written bin must deserialize as HLL in Java");
        byte[] fromServer = hll.getBytes();
        byte[] fromB64 = Base64.getDecoder().decode(rec.getString(WIRE_B64_BIN));
        assertArrayEquals(fromB64, fromServer);

        RecordStream countRs = session.upsert(key)
            .bin(BIN).hllGetCount()
            .execute();
        Record countRec = countRs.getFirstRecord();
        assertNotNull(countRec);
        long count = countRec.getLong(BIN);
        assertTrue(count >= 2 && count <= 4, "cardinality estimate near 3; got " + count);
    }
}
