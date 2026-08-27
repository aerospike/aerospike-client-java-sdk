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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;

/**
 * Client-side {@link QueryBuilder} validation. Uses {@link ClusterTest#session} for
 * construction only; no server I/O.
 */
public class QueryBuilderValidationTest extends ClusterTest {
    private static final DataSet dataSet = DataSet.of("test", "qbval");

    private QueryBuilder qb;

    @BeforeEach
    void newBuilder() {
        qb = new QueryBuilder(session, dataSet);
    }

    @Test
    void secondPartitionRangeThrows() {
        qb.onPartitionRange(0, 100);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> qb.onPartitionRange(200, 300));

        assertTrue(ex.getMessage().contains("Partition range can only be defined once"));
    }

    @Test
    void startPartitionBelowZeroThrows() {
        assertThrows(IllegalArgumentException.class, () -> qb.onPartitionRange(-1, 1));
    }

    @Test
    void startPartitionAtOrAbove4096Throws() {
        assertThrows(IllegalArgumentException.class, () -> qb.onPartitionRange(4096, 4097));
    }

    @Test
    void endPartitionBelowOneThrows() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> qb.onPartitionRange(0, 0));

        assertTrue(ex.getMessage().contains("End partition"));
    }

    @Test
    void endPartitionAboveMaxShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> qb.onPartitionRange(0, 99_999));
    }

    @Test
    void startPartitionNotLessThanEndThrows() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> qb.onPartitionRange(100, 100));

        assertTrue(ex.getMessage().contains("Start partition must be less than the end partition"));
    }

    @Test
    void withNoBinsAfterReadingOnlyBinsThrows() {
        qb.readingOnlyBins("a");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, qb::withNoBins);

        assertTrue(ex.getMessage().contains("withNoBins"));
    }

    @Test
    void withHintTwiceThrows() {
        qb.withHint(hint -> hint.forIndex("idx"));

        assertThrows(IllegalArgumentException.class,
            () -> qb.withHint(hint -> hint.forBin("age")));
    }

    @Test
    void limitZeroThrows() {
        assertThrows(IllegalArgumentException.class, () -> qb.limit(0));
    }

    @Test
    void chunkSizeZeroThrows() {
        assertThrows(IllegalArgumentException.class, () -> qb.chunkSize(0));
    }
}
