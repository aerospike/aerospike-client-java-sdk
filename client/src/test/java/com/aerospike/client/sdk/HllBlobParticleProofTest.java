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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Value.HLLValue;

/**
 * Proof for claim (B): valid HLL <strong>register bytes</strong> written as a
 * generic byte blob (no HLL particle) produce a server bin that is <strong>not
 * an HLL</strong> — HLL read ops fail with {@link ResultCode#BIN_TYPE_ERROR} and
 * point reads expose raw bytes, not {@link HLLValue}.
 * <p>
 * Why {@code CrossClientHllInteropTest} still saw HLL everywhere: those tests used
 * {@code hllAdd} / {@code HllConfig}, which send the HLL particle. This class
 * covers the <strong>raw bytes / {@code setTo(byte[])} / Python {@code put} with
 * {@code bytearray}</strong> path.
 * <p>
 * Optional cross-step: run {@code test_hll_blob_put_particle_type.py} first, then
 * {@link #javaReadsPythonBytearrayPutBin_asBlobNotHll} (same user key
 * {@value #PYTHON_PUT_USER_KEY} on set {@value #DEMO_SET}).
 */
public class HllBlobParticleProofTest extends ClusterTest {

    private static final String DEMO_SET = "demo";
    private static final String BIN = "hllbin";
    /** Same user key as {@code test_hll_blob_put_particle_type.KEY_BLOB} in Python. */
    private static final String PYTHON_PUT_USER_KEY = "hllBlobParticlePythonPut";

    private Key demoKey(String userKey) {
        return DataSet.of(args.namespace, DEMO_SET).id(userKey);
    }

    /**
     * Self-contained: Java writes HLL wire via {@code hllAdd}, rewrites the same bin
     * with raw {@code byte[]} (blob particle), then read + HLL describe must fail.
     */
    @Test
    public void sameWireWrittenAsRawBytes_readIsNotHllValue_hllDescribeFails() {
        Key key = demoKey("hllBlobProofJavaOnly");
        session.delete(key).execute();

        session.upsert(key)
            .bin(BIN).hllAdd(new ArrayList<>(Arrays.asList("p", "q")), HllConfig.of(8))
            .execute();

        Record rec = session.upsert(key)
            .bin(BIN).get()
            .execute()
            .getFirstRecord();
        HLLValue hll = rec.getHLLValue(BIN);
        assertNotNull(hll);
        byte[] wire = hll.getBytes();

        session.delete(key).execute();
        session.upsert(key)
            .bin(BIN).setTo(wire)
            .execute();

        Record blobRec = session.query(key)
            .readingOnlyBins(BIN)
            .execute()
            .getFirstRecord();

        Object v = blobRec.getValue(BIN);
        assertFalse(v instanceof HLLValue, "blob bin must not deserialize as HLLValue");

        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            session.upsert(key)
                .bin(BIN).hllDescribe()
                .execute()
                .getFirstRecord());
        assertEquals(ResultCode.BIN_TYPE_ERROR, ex.getResultCode());
    }

    /**
     * After Python {@code put(..., {hllbin: bytearray(wire)})} on this key, Java
     * must see a non-HLL bin and HLL describe must fail — same server state as
     * {@link #sameWireWrittenAsRawBytes_readIsNotHllValue_hllDescribeFails}.
     */
    @Test
    public void javaReadsPythonBytearrayPutBin_asBlobNotHll() {
        Key key = demoKey(PYTHON_PUT_USER_KEY);
        Record rec = session.query(key)
            .readingOnlyBins(BIN)
            .execute()
            .getFirstRecord();
        Assumptions.assumeTrue(rec != null,
            () -> "Run test_hll_blob_put_particle_type.py first (writes this key).");

        Object v = rec.getValue(BIN);
        assertFalse(v instanceof HLLValue);

        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            session.upsert(key)
                .bin(BIN).hllDescribe()
                .execute()
                .getFirstRecord());
        assertEquals(ResultCode.BIN_TYPE_ERROR, ex.getResultCode());
    }
}
