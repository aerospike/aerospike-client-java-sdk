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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@code executeAsync(...).asCompletableFuture*()} passive drain behavior.
 */
public class ExecuteCompletableFutureTest extends ClusterTest {

    private static final String KEY_PREFIX = "exec_cf_";

    @BeforeAll
    public static void setupRecords() {
        for (int i = 0; i < 3; i++) {
            session.upsert(args.set.id(KEY_PREFIX + i))
                .bin("name").setTo("user_" + i)
                .bin("val").setTo(i)
                .execute();
        }
    }

    @Test
    public void executeCompletableFutureBatchQuery() {
        List<RecordResult> results = session.query(args.set.ids(
                KEY_PREFIX + "0", KEY_PREFIX + "1", KEY_PREFIX + "2"))
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFuture()
            .join();

        assertEquals(3, results.size());
        assertTrue(results.stream().allMatch(RecordResult::isOk));
    }

    @Test
    public void executeCompletableFutureSingleUpdate() {
        Key key = args.set.id(KEY_PREFIX + "0");

        Optional<RecordResult> result = session.update(key)
            .bin("val").add(10)
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle()
            .join();

        assertTrue(result.isPresent());
        assertTrue(result.get().isOk());
    }

    @Test
    public void executeCompletableFutureSingleRejectsMultiKey() {
        CompletionException ex = assertThrows(CompletionException.class, () ->
            session.update(args.set.ids(KEY_PREFIX + "0", KEY_PREFIX + "1"))
                .bin("val").add(1)
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFutureSingle()
                .join());
        assertInstanceOf(IllegalStateException.class, ex.getCause());
    }

    @Test
    public void executeCompletableFutureSingleEmptyForMissingKey() {
        Optional<RecordResult> result = session.query(args.set.id(KEY_PREFIX + "no_such_key"))
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle()
            .join();

        assertTrue(result.isEmpty());
    }

    @Test
    public void chainedSingleUpdatesViaThenCompose() {
        Key key0 = args.set.id(KEY_PREFIX + "0");
        Key key1 = args.set.id(KEY_PREFIX + "1");

        CompletableFuture<Optional<RecordResult>> chain = session.update(key0)
            .bin("val").add(1)
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle()
            .thenCompose(ignored -> session.update(key1)
                .bin("val").add(1)
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFutureSingle());

        Optional<RecordResult> second = chain.join();
        assertTrue(second.isPresent());
        assertTrue(second.get().isOk());
        assertEquals(key1, second.get().getKey());
    }

    @Test
    public void concurrentSingleUpdates() {
        Key key0 = args.set.id(KEY_PREFIX + "0");
        Key key1 = args.set.id(KEY_PREFIX + "1");
        Key key2 = args.set.id(KEY_PREFIX + "2");

        CompletableFuture<Optional<RecordResult>> f0 = session.update(key0).bin("val").add(1)
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle();
        CompletableFuture<Optional<RecordResult>> f1 = session.update(key1).bin("val").add(1)
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle();
        CompletableFuture<Optional<RecordResult>> f2 = session.update(key2).bin("val").add(1)
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFutureSingle();

        CompletableFuture.allOf(f0, f1, f2).join();
        assertTrue(f0.join().orElseThrow().isOk());
        assertTrue(f1.join().orElseThrow().isOk());
        assertTrue(f2.join().orElseThrow().isOk());
    }

    @Test
    public void executeCompletableFutureWithErrorHandler() {
        AtomicInteger errorCount = new AtomicInteger();

        List<RecordResult> results = session.query(args.set.ids(KEY_PREFIX + "0", KEY_PREFIX + "missing"))
            .includeMissingKeys()
            .executeAsync((key, index, err) -> errorCount.incrementAndGet())
            .asCompletableFuture()
            .join();

        assertEquals(1, results.size());
        assertEquals(1, errorCount.get());
    }

    @Test
    public void passiveDrainMatchesAsyncStream() {
        List<RecordResult> viaBuilder = session.query(args.set.ids(KEY_PREFIX + "0", KEY_PREFIX + "1"))
            .executeAsync(ErrorStrategy.IN_STREAM)
            .asCompletableFuture()
            .join();

        List<RecordResult> viaStream;
        try (RecordStream rs = session.query(args.set.ids(KEY_PREFIX + "0", KEY_PREFIX + "1"))
                .executeAsync(ErrorStrategy.IN_STREAM)) {
            viaStream = rs.asCompletableFuture().join();
        }

        assertEquals(viaStream.size(), viaBuilder.size());
        for (int i = 0; i < viaStream.size(); i++) {
            assertEquals(viaStream.get(i).getKey(), viaBuilder.get(i).getKey());
        }
    }
}
