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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.plan.IndexRangeWire;

/**
 * Wire-encoding coverage for {@link Filter}: opaque {@link Filter#fromWireRange} replay on the
 * server-led query path, and structured factory bytes for legacy callers.
 */
public class FilterWireRangeTest {

    private static final String GEO_POINT =
        "{\"type\":\"Point\",\"coordinates\":[-122.0986857,37.4214209]}";
    private static final String GEO_REGION =
        "{\"type\":\"Polygon\",\"coordinates\":[[[-122.1,37.4],[-122.0,37.4],[-122.0,37.5],[-122.1,37.5],[-122.1,37.4]]]}";
    private static final String GEO_CIRCLE =
        "{ \"type\": \"AeroCircle\", "
            + "\"coordinates\": [[-122.10000000, 37.40000000], 1000.000000] }";
    private static final byte[] BLOB = new byte[] {1, 2, 3, 4};

    @Test
    void fromWireRangeReplaysBytesVerbatim() {
        Filter structured = Filter.equal("age", 30L);
        byte[] wireBody = new byte[1 + structured.estimateSize()];
        wireBody[0] = 1;
        structured.write(wireBody, 1);

        Filter opaque = Filter.fromWireRange("age_idx", wireBody);
        assertTrue(opaque.hasWireRange());
        assertEquals("age_idx", opaque.getIndexName());
        assertEquals(wireBody.length, opaque.estimateSize());

        byte[] out = new byte[wireBody.length];
        opaque.write(out, 0);
        assertArrayEquals(wireBody, out);

        Filter mapKeysOpaque = Filter.fromWireRange("mapkeys_idx", wireBody, IndexCollectionType.MAPKEYS);
        assertEquals(IndexCollectionType.MAPKEYS, mapKeysOpaque.getCollectionType());
    }

    @Test
    void structuredFiltersEncodeProbeRangeBytes() {
        assertProbeRange(Filter.equal("age", 30L), "bin=age range=[30,30]");
        assertProbeRange(Filter.range("age", 18L, 65L), "bin=age range=[18,65]");
        assertProbeRange(Filter.equal("name", "alice"), "bin=name value=alice len=5");
        assertProbeRange(Filter.equal("digest", BLOB), "bin=digest value=x'01020304' len=4");
        assertProbeRange(Filter.geoWithinRegion("loc", GEO_REGION),
            "bin=loc region=" + GEO_REGION + " len=" + GEO_REGION.length());
        assertProbeRange(Filter.geoWithinRadius("loc", -122.1, 37.4, 1000.0),
            "bin=loc region=" + GEO_CIRCLE + " len=" + GEO_CIRCLE.length());
        assertProbeRange(Filter.geoContains("loc", GEO_POINT),
            "bin=loc region=" + GEO_POINT + " len=" + GEO_POINT.length());

        // packedExp is not written to INDEX_RANGE; this case only checks the empty-bin-name wire shape.
        assertProbeRange(Filter.equal(ageIntIndexExpression(), 30L), "bin= range=[30,30]");
        assertProbeRange(Filter.equalByIndex("age_idx", 30L), "bin= range=[30,30]");
    }

    @Test
    void toStringIncludesStructuredFields() {
        Filter filter = Filter.range("scores", IndexCollectionType.LIST, 10L, 20L, CTX.listIndex(0));

        assertTrue(filter.toString().contains("Filter{name=scores"));
        assertTrue(filter.toString().contains("indexName=null"));
        assertTrue(filter.toString().contains("colType=LIST"));
        assertTrue(filter.toString().contains("valType=" + ParticleType.INTEGER));
        assertTrue(filter.toString().contains("begin=10"));
        assertTrue(filter.toString().contains("end=20"));
        assertTrue(filter.toString().contains("packedCtx=bytes[len="));
    }

    @Test
    void toStringIncludesWireRangeBytes() {
        byte[] wireBody = new byte[] {1, 2, 3};
        Filter filter = Filter.fromWireRange("age_idx", wireBody, IndexCollectionType.MAPKEYS);

        assertEquals(
            "Filter{name=null, indexName=age_idx, colType=MAPKEYS, valType=0, wireRangeBytes=bytes[len=3, hex=010203]}",
            filter.toString()
        );
    }

    private static void assertProbeRange(Filter filter, String expectedDescription) {
        assertFalse(filter.hasWireRange());
        byte[] wireBody = new byte[1 + filter.estimateSize()];
        wireBody[0] = 1;
        filter.write(wireBody, 1);
        assertEquals(expectedDescription, IndexRangeWire.describeProbeRange(wireBody));
    }

    private static Expression ageIntIndexExpression() {
        return Exp.build(Exp.intBin("age"));
    }
}
