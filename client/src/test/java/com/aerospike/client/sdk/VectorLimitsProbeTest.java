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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.vector.Vector;

/**
 * Server-behavior tests for vector dimension limits.
 * <p>
 * Confirmed against the dev server (8.1.3 vector build): the server enforces
 * {@code 1 <= dimensions <= VECTOR_MAX_ELEMENTS_BYTES / elementSize} (for fp64,
 * {@code VECTOR_MAX_ELEMENTS_BYTES = 1 << 18} gives 32768 dims). Empty and over-maximum vectors are
 * rejected cleanly with {@link ResultCode#PARAMETER_ERROR}.
 * <p>
 * TODO: consider validating empty and per-type maximum dimensions in {@code Vector.of*()}.
 */
public class VectorLimitsProbeTest extends ClusterTest {
    private static final String binName = "vecbin";

    // Server cap: VECTOR_MAX_ELEMENTS_BYTES = 1 << 18 (262144). fp64 => 32768 dims max.
    private static final int FLOAT64_MAX_DIMS = (1 << 18) / 8;

    @Test
    public void emptyVectorRejectedCleanly() {
        final Key key = key("vecempty");

        final AerospikeException ae = assertThrows(AerospikeException.class, () ->
            session.upsert(key).bin(binName).setTo(Vector.ofFloat32(new float[0])).execute());
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());

        assertNodeAlive();
    }

    @Test
    public void overMaxDimensionsRejectedCleanly() {
        final Key key = key("vecovermax");

        final AerospikeException ae = assertThrows(AerospikeException.class, () ->
            session.upsert(key)
                .bin(binName).setTo(Vector.ofFloat64(new double[FLOAT64_MAX_DIMS + 1]))
                .execute());
        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());

        assertNodeAlive();
    }

    @Test
    public void atMaxDimensionsRoundTrips() {
        final Key key = key("vecatmax");

        final Vector v = Vector.ofFloat64(new double[FLOAT64_MAX_DIMS]);
        session.upsert(key).bin(binName).setTo(v).execute();

        final Vector got = session.query(key).execute().getFirstRecord().getVector(binName);
        assertEquals(FLOAT64_MAX_DIMS, got.dimensions);
        assertEquals(v, got);
    }

    /** Confirm the node remains available after a rejected write. */
    private void assertNodeAlive() {
        final Key key = key("vecprobe_alive");
        session.upsert(key).bin("ok").setTo(1).execute();
        assertEquals(1, session.query(key).execute().getFirstRecord().getInt("ok"));
    }

    private Key key(final String id) {
        final Key k = args.set.id(id);
        session.delete(k).execute();
        return k;
    }
}
