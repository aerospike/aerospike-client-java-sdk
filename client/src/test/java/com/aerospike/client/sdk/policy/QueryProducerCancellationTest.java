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
package com.aerospike.client.sdk.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;

/**
 * Tests that verify producers (simulating QueryNodeExecutor.parseRow()) are
 * stopped promptly when the consumer closes the stream.
 *
 * <p>QueryNodeExecutor.parseRow() publishes each record to the AsyncRecordStream
 * and then checks {@code stream.cancelled().getAsBoolean()}. If cancelled, it
 * calls {@code stop()} and throws {@code QueryTerminated} to halt further wire reads.</p>
 *
 * <p>Two scenarios are tested:
 * <ul>
 *   <li><b>Small result set</b> — total records fit within the queue capacity,
 *       so the producer never blocks on backpressure.</li>
 *   <li><b>Large result set</b> — total records exceed the queue capacity,
 *       so the producer blocks on backpressure until the consumer drains items.</li>
 * </ul>
 */
public class QueryProducerCancellationTest {

    private RecordResult createResult(int id) {
        Key key = new Key("test", "set", id);
        Record record = new Record(0, 0);
        return new RecordResult(key, record, 0);
    }

    /**
     * Simulates QueryNodeExecutor.parseRow() behaviour on a producer thread.
     * Publishes records one at a time and checks cancelled() after each publish,
     * exactly as the real parseRow() does. Returns the number of records published
     * before the producer stopped (either by finishing or by detecting cancellation).
     *
     * @param stream       the stream to publish into
     * @param totalRecords number of records to attempt to publish
     * @param published    counter incremented after each successful publish
     * @param terminated   set to true if the producer detected cancellation
     * @param pauseAfter   if >= 0, the producer signals {@code readyToClose} and
     *                     waits for {@code closeComplete} after publishing this many items
     * @param readyToClose latch signalled when the producer has published {@code pauseAfter} items
     * @param closeComplete latch the producer waits on before resuming after the pause
     */
    private void simulateParseRowProducer(
        AsyncRecordStream stream,
        int totalRecords,
        AtomicInteger published,
        AtomicBoolean terminated,
        int pauseAfter,
        CountDownLatch readyToClose,
        CountDownLatch closeComplete
    ) {
        for (int i = 0; i < totalRecords; i++) {
            stream.publish(createResult(i));
            published.incrementAndGet();

            if (stream.cancelled().getAsBoolean()) {
                terminated.set(true);
                return;
            }

            if (pauseAfter >= 0 && published.get() == pauseAfter) {
                readyToClose.countDown();
                try {
                    closeComplete.await(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        stream.complete();
    }

    // ========================================
    // Small result set: items < queue capacity
    // ========================================

    /**
     * When the total number of records is smaller than the queue capacity,
     * the producer never experiences backpressure. Items flow straight into the
     * queue buffer without blocking.
     *
     * <p>We use explicit synchronisation (latches) to guarantee that close() is
     * called while the producer still has records left to publish. Without this,
     * the producer could finish before close() is called because nothing blocks it.</p>
     */
    @Test
    public void closeStopsProducer_resultsSmallerThanQueueCapacity() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 100;
            int totalRecords = 50;
            int pauseAfterPublishing = 5;

            AsyncRecordStream stream = new AsyncRecordStream(queueCapacity);
            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);
            CountDownLatch readyToClose = new CountDownLatch(1);
            CountDownLatch closeComplete = new CountDownLatch(1);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(stream, totalRecords, published, terminated,
                    pauseAfterPublishing, readyToClose, closeComplete)
            );

            assertTrue(readyToClose.await(2, TimeUnit.SECONDS),
                "Producer should have published enough items");

            // Consume a few items from the queue
            for (int i = 0; i < 3; i++) {
                assertTrue(stream.hasNext());
                stream.next();
            }

            // Close the stream — sets cancelled flag
            stream.close();
            closeComplete.countDown();

            producer.join(2000);
            assertFalse(producer.isAlive(), "Producer thread should have exited");

            assertTrue(terminated.get(),
                "Producer should have detected cancellation via cancelled()");
            assertTrue(published.get() < totalRecords,
                "Producer should have stopped early; published " + published.get() + " of "
                    + totalRecords);
            // The producer published exactly pauseAfterPublishing items, then paused.
            // After close, it resumes, publishes one more, and then cancelled() returns true.
            assertEquals(pauseAfterPublishing + 1, published.get(),
                "Producer should have published exactly " + (pauseAfterPublishing + 1) +
                " items (pause point + 1 more before detecting cancellation)");
        });
    }

    /**
     * Verifies the same small-result-set scenario through the RecordStream wrapper,
     * ensuring the full close() propagation chain works end-to-end.
     */
    @Test
    public void closeStopsProducer_resultsSmallerThanQueueCapacity_viaRecordStream() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 100;
            int totalRecords = 50;
            int pauseAfterPublishing = 5;

            AsyncRecordStream asyncStream = new AsyncRecordStream(queueCapacity);
            RecordStream recordStream = new RecordStream(asyncStream);

            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);
            CountDownLatch readyToClose = new CountDownLatch(1);
            CountDownLatch closeComplete = new CountDownLatch(1);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(asyncStream, totalRecords, published, terminated,
                    pauseAfterPublishing, readyToClose, closeComplete)
            );

            assertTrue(readyToClose.await(2, TimeUnit.SECONDS));

            // Consume via RecordStream API
            assertTrue(recordStream.hasNext());
            recordStream.next();

            // Close via RecordStream — should propagate to AsyncRecordStream
            recordStream.close();
            closeComplete.countDown();

            producer.join(2000);
            assertFalse(producer.isAlive());

            assertTrue(terminated.get(),
                "Producer should have detected cancellation via RecordStream.close()");
            assertTrue(published.get() < totalRecords);
        });
    }

    // ========================================
    // Large result set: items > queue capacity
    // ========================================

    /**
     * When the total number of records exceeds the queue capacity, the producer
     * blocks on backpressure when the queue is full. Closing the stream clears
     * the queue (unblocking the producer), and the cancelled() check on the
     * next iteration causes the producer to stop.
     *
     * <p>No artificial synchronisation is needed: backpressure itself coordinates
     * the producer and consumer.</p>
     */
    @Test
    public void closeStopsProducer_resultsLargerThanQueueCapacity() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 5;
            int totalRecords = 10_000;

            AsyncRecordStream stream = new AsyncRecordStream(queueCapacity);
            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(stream, totalRecords, published, terminated,
                    -1, null, null)
            );

            // Consume a few items so the producer can make progress
            for (int i = 0; i < 3; i++) {
                assertTrue(stream.hasNext());
                stream.next();
            }

            // Close the stream while the producer is blocked on backpressure
            stream.close();

            producer.join(2000);
            assertFalse(producer.isAlive(), "Producer thread should have exited");

            assertTrue(terminated.get(),
                "Producer should have detected cancellation via cancelled()");
            // The producer can have published at most queueCapacity+1 items (queue fills up)
            // plus a few more after close() clears the queue, but certainly far fewer than 10,000.
            assertTrue(published.get() < totalRecords,
                "Producer should have stopped early; published " + published.get() + " of "
                    + totalRecords);
            assertTrue(published.get() <= queueCapacity + 10,
                "Producer should not have published much beyond queue capacity; published "
                    + published.get());
        });
    }

    /**
     * Same large-result-set scenario through RecordStream.getFirst(), verifying
     * that the terminal auto-close mechanism propagates cancellation to the producer.
     */
    @SuppressWarnings("resource") // getFirst() auto-closes the stream
    @Test
    public void getFirstClosesStreamAndStopsProducer() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 5;
            int totalRecords = 10_000;

            AsyncRecordStream asyncStream = new AsyncRecordStream(queueCapacity);
            RecordStream recordStream = new RecordStream(asyncStream);

            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(asyncStream, totalRecords, published, terminated,
                    -1, null, null)
            );

            // getFirst() retrieves one item and auto-closes the stream
            recordStream.getFirst();

            producer.join(2000);
            assertFalse(producer.isAlive(), "Producer thread should have exited after getFirst()");

            assertTrue(terminated.get(),
                "Producer should have detected cancellation after getFirst() closed the stream");
            assertTrue(published.get() < totalRecords);
        });
    }

    /**
     * Verifies that consuming all records naturally (without calling close()) also
     * allows the producer to finish cleanly via complete(), and that the producer
     * is NOT flagged as terminated.
     */
    @Test
    public void fullConsumption_producerCompletesNormally() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 5;
            int totalRecords = 20;

            AsyncRecordStream stream = new AsyncRecordStream(queueCapacity);
            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(stream, totalRecords, published, terminated,
                    -1, null, null)
            );

            // Consume ALL records
            int consumed = 0;
            while (stream.hasNext()) {
                stream.next();
                consumed++;
            }

            producer.join(2000);
            assertFalse(producer.isAlive());

            assertFalse(terminated.get(),
                "Producer should NOT have been terminated — it completed naturally");
            assertEquals(totalRecords, published.get(),
                "All records should have been published");
            assertEquals(totalRecords, consumed,
                "All records should have been consumed");
        });
    }

    /**
     * Verifies that forEach (which auto-closes) stops the producer after
     * processing only some records if the consumer throws an exception.
     */
    @SuppressWarnings("resource") // forEach() auto-closes the stream
    @Test
    public void forEachClosesStreamAndStopsProducerOnException() {
        Assertions.assertTimeout(Duration.ofMillis(3000), () -> {
            int queueCapacity = 5;
            int totalRecords = 10_000;
            int throwAfter = 3;

            AsyncRecordStream asyncStream = new AsyncRecordStream(queueCapacity);
            RecordStream recordStream = new RecordStream(asyncStream);

            AtomicInteger published = new AtomicInteger(0);
            AtomicBoolean terminated = new AtomicBoolean(false);
            AtomicInteger consumed = new AtomicInteger(0);

            Thread producer = Thread.ofVirtual().start(() ->
                simulateParseRowProducer(asyncStream, totalRecords, published, terminated,
                    -1, null, null)
            );

            try {
                recordStream.forEach(r -> {
                    if (consumed.incrementAndGet() >= throwAfter) {
                        throw new RuntimeException("Stop processing");
                    }
                });
            }
            catch (RuntimeException e) {
                assertEquals("Stop processing", e.getMessage());
            }

            producer.join(2000);
            assertFalse(producer.isAlive(),
                "Producer should have exited after forEach closed the stream");

            assertTrue(terminated.get(),
                "Producer should have detected cancellation after forEach's finally block closed the stream");
            assertTrue(published.get() < totalRecords);
        });
    }
}
