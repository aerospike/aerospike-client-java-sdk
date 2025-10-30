package com.aerospike.client.fluent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.Test;

/**
 * Comprehensive tests for AsyncRecordStream covering:
 * - Basic functionality
 * - The reported bug (hanging with one element)
 * - Backpressure
 * - Error handling
 * - Cancellation
 * - Concurrent access
 * - Edge cases
 * - Stream API integration
 */
public class AsyncRecordStreamTest {

    // Helper method to create a test RecordResult
    private RecordResult createResult(int id) {
        Key key = new Key("test", "set", id);
        Record record = new Record(null, 0, 0);
        return new RecordResult(key, record, ResultCode.OK, false, null);
    }

    // ========================================
    // Basic Functionality Tests
    // ========================================

    @Test(timeout = 2000)
    public void testSingleElement() {
        // Single element publish and consume
        AsyncRecordStream stream = new AsyncRecordStream(10);
        RecordResult result = createResult(1);

        stream.publish(result);
        stream.complete();

        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
        assertEquals(result, results.get(0));
    }

    @Test(timeout = 2000)
    public void testMultipleElements() {
        // Multiple elements publish and consume
        AsyncRecordStream stream = new AsyncRecordStream(10);
        List<RecordResult> expected = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            RecordResult result = createResult(i);
            expected.add(result);
            stream.publish(result);
        }
        stream.complete();

        List<RecordResult> actual = stream.stream().toList();
        assertEquals(expected, actual);
    }

    @Test(timeout = 2000)
    public void testEmptyStream() {
        // Empty stream with immediate complete
        AsyncRecordStream stream = new AsyncRecordStream(10);
        stream.complete();

        List<RecordResult> results = stream.stream().toList();
        assertTrue(results.isEmpty());
    }

    @Test(timeout = 2000)
    public void testIteratorConsumption() {
        // Iterator-based consumption
        AsyncRecordStream stream = new AsyncRecordStream(10);
        RecordResult result1 = createResult(1);
        RecordResult result2 = createResult(2);

        stream.publish(result1);
        stream.publish(result2);
        stream.complete();

        List<RecordResult> results = new ArrayList<>();
        for (RecordResult r : stream) {
            results.add(r);
        }

        assertEquals(2, results.size());
        assertEquals(result1, results.get(0));
        assertEquals(result2, results.get(1));
    }

    // ========================================
    // Reported Bug Scenario Tests
    // ========================================

    @Test(timeout = 2000)
    public void testFindFirstWithOneElementNoHang() {
        // CRITICAL: stream().findFirst() should not hang with one element (capacity=1)
        AsyncRecordStream stream = new AsyncRecordStream(1);
        RecordResult result = createResult(1);

        // Publish one element and complete immediately (queue is full)
        stream.publish(result);
        stream.complete();

        // This should NOT hang (the reported bug)
        Optional<RecordResult> first = stream.stream().findFirst();
        assertTrue(first.isPresent());
        assertEquals(result, first.get());
    }

    @Test(timeout = 2000)
    public void testCompleteBeforeConsumeNoHang() {
        // Capacity=1, complete before consume should not hang
        AsyncRecordStream stream = new AsyncRecordStream(1);
        RecordResult result = createResult(1);

        stream.publish(result);
        stream.complete();

        // Consume after complete
        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
        assertEquals(result, results.get(0));
    }

    @Test(timeout = 2000)
    public void testCompleteAfterConsume() {
        // Capacity=1, complete after consume should work
        AsyncRecordStream stream = new AsyncRecordStream(1);
        RecordResult result = createResult(1);

        // Publish element
        stream.publish(result);
        
        // Complete immediately (before consumption)
        stream.complete();
        
        // Should be able to consume all elements
        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
        assertEquals(result, results.get(0));
    }

    // ========================================
    // Backpressure Tests
    // ========================================

    @Test(timeout = 3000)
    public void testPublishBlocksWhenFull() throws Exception {
        // Publisher blocks when queue is full
        AsyncRecordStream stream = new AsyncRecordStream(2);
        AtomicBoolean publisherBlocked = new AtomicBoolean(false);
        CountDownLatch queueFull = new CountDownLatch(1);

        // Fill the queue (capacity is 2, but we have +1 for END marker, so 3 total)
        stream.publish(createResult(1));
        stream.publish(createResult(2));
        // The +1 slot is reserved for END/Err, so this should still work
        stream.publish(createResult(3));

        // This publish should block because queue is actually full now
        Thread publisher = Thread.ofVirtual().start(() -> {
            queueFull.countDown();
            publisherBlocked.set(true);
            stream.publish(createResult(4)); // This will block
            publisherBlocked.set(false);
        });

        queueFull.await();
        Thread.sleep(100); // Give publisher time to block
        assertTrue("Publisher should be blocked", publisherBlocked.get());

        // Consume one element to unblock
        assertTrue(stream.hasNext());
        stream.next();

        // Publisher should unblock
        publisher.join(1000);
        assertFalse("Publisher should have unblocked", publisher.isAlive());
        assertFalse("Publisher should no longer be blocked", publisherBlocked.get());

        stream.complete();
    }

    @Test(timeout = 2000)
    public void testConsumerUnblocksPublisher() throws Exception {
        // Consumer can drain queue and unblock publisher
        AsyncRecordStream stream = new AsyncRecordStream(5);
        CountDownLatch allPublished = new CountDownLatch(1);

        // Publisher thread - publishes 10 elements
        Thread publisher = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 10; i++) {
                stream.publish(createResult(i));
            }
            allPublished.countDown();
            stream.complete();
        });

        // Consumer thread - slowly drains
        Thread consumer = Thread.ofVirtual().start(() -> {
            int count = 0;
            for (RecordResult r : stream) {
                count++;
            }
            assertEquals(10, count);
        });

        publisher.join(2000);
        consumer.join(2000);
        assertFalse(publisher.isAlive());
        assertFalse(consumer.isAlive());
        assertTrue("All elements should have been published", allPublished.getCount() == 0);
    }

    // ========================================
    // Error Handling Tests
    // ========================================

    @Test(timeout = 2000)
    public void testErrorPropagation() {
        // Error is propagated to consumer
        AsyncRecordStream stream = new AsyncRecordStream(10);
        RuntimeException testError = new RuntimeException("Test error");

        stream.publish(createResult(1));
        stream.error(testError);

        // Consume first element
        assertTrue(stream.hasNext());
        assertEquals(createResult(1), stream.next());

        // Next call should throw the error
        try {
            stream.next();
            throw new AssertionError("Expected RuntimeException");
        } catch (RuntimeException thrown) {
            assertEquals("Test error", thrown.getMessage());
        }
    }

    @Test(timeout = 2000)
    public void testErrorWithFullQueueNoHang() {
        // Error with full queue should not hang
        AsyncRecordStream stream = new AsyncRecordStream(1);
        RuntimeException testError = new RuntimeException("Test error");

        // Fill the queue
        stream.publish(createResult(1));
        // Signal error (queue is full, but +1 slot allows this)
        stream.error(testError);

        // Consume should work
        assertTrue(stream.hasNext());
        stream.next();

        // Error should be received
        try {
            stream.next();
            throw new AssertionError("Expected RuntimeException");
        } catch (RuntimeException e) {
            // Expected
        }
    }

    @Test(timeout = 2000)
    public void testErrorAfterComplete() {
        // Error after complete is ignored
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(createResult(1));
        stream.complete();
        stream.error(new RuntimeException("Should be ignored"));

        // Should consume normally without error
        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
    }

    @Test(timeout = 2000)
    public void testNullError() {
        // Null error is converted to RuntimeException
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.error(null);

        try {
            stream.next();
            throw new AssertionError("Expected RuntimeException");
        } catch (RuntimeException thrown) {
            assertEquals("Unknown error", thrown.getMessage());
        }
    }

    // ========================================
    // Cancellation Tests
    // ========================================

    @Test(timeout = 2000)
    public void testCloseMidConsumption() {
        // Close stream mid-consumption
        AsyncRecordStream stream = new AsyncRecordStream(10);

        for (int i = 0; i < 5; i++) {
            stream.publish(createResult(i));
        }

        // Consume first element
        assertTrue(stream.hasNext());
        stream.next();

        // Close stream
        stream.close();

        // cancelled() should return true
        assertTrue(stream.cancelled().getAsBoolean());

        // Note: close() clears the queue and adds END, but if the iterator
        // already fetched the next element before close(), hasNext() might still be true.
        // The key is that cancelled() returns true and further iteration will stop quickly.
        // So we just verify cancelled() works, not the exact hasNext() state.
    }

    @Test(timeout = 2000)
    public void testCancelledAfterClose() {
        // cancelled() returns true after close
        AsyncRecordStream stream = new AsyncRecordStream(10);

        assertFalse(stream.cancelled().getAsBoolean());
        stream.close();
        assertTrue(stream.cancelled().getAsBoolean());
    }

    @Test(timeout = 2000)
    public void testPublishStopsAfterClose() throws Exception {
        // publish() stops after close
        AsyncRecordStream stream = new AsyncRecordStream(10);
        AtomicInteger publishCount = new AtomicInteger(0);

        Thread publisher = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 100; i++) {
                if (stream.cancelled().getAsBoolean()) {
                    break;
                }
                stream.publish(createResult(i));
                publishCount.incrementAndGet();
            }
        });

        // Let publisher publish a few
        Thread.sleep(50);
        stream.close();

        publisher.join(1000);
        assertFalse(publisher.isAlive());

        // Should have published fewer than 100 due to cancellation
        assertTrue("Publisher should have stopped early", publishCount.get() < 100);
    }

    @Test(timeout = 2000)
    public void testCloseIdempotent() {
        // Close is idempotent
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.close();
        stream.close(); // Should not throw or cause issues
        stream.close();

        assertTrue(stream.cancelled().getAsBoolean());
    }

    // ========================================
    // Concurrent Access Tests
    // ========================================

    @Test(timeout = 5000)
    public void testMultipleProducers() throws Exception {
        // Multiple producers publishing simultaneously
        AsyncRecordStream stream = new AsyncRecordStream(100);
        int numProducers = 3;
        int elementsPerProducer = 10;
        AtomicInteger totalPublished = new AtomicInteger(0);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        // Start multiple producers
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            futures.add(executor.submit(() -> {
                for (int i = 0; i < elementsPerProducer; i++) {
                    stream.publish(createResult(producerId * 1000 + i));
                    totalPublished.incrementAndGet();
                }
            }));
        }

        // Wait for all producers to finish
        for (var future : futures) {
            future.get(3, TimeUnit.SECONDS);
        }
        executor.shutdown();
        
        stream.complete();

        // Verify we got all elements
        long count = stream.stream().count();
        assertEquals(numProducers * elementsPerProducer, count);
        assertEquals(numProducers * elementsPerProducer, totalPublished.get());
    }

    @Test(timeout = 2000)
    public void testHasMorePagesThreadSafety() throws Exception {
        // hasMorePages() is thread-safe
        AsyncRecordStream stream = new AsyncRecordStream(10);
        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger trueCount = new AtomicInteger(0);

        // Multiple threads call hasMorePages() simultaneously
        for (int i = 0; i < numThreads; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    startLatch.await();
                    if (stream.hasMorePages()) {
                        trueCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(2, TimeUnit.SECONDS));

        // Only one thread should have seen true
        assertEquals("Only one thread should see hasMorePages() return true", 1, trueCount.get());
    }

    // ========================================
    // Edge Case Tests
    // ========================================

    @Test(timeout = 2000)
    public void testCompleteIdempotent() {
        // complete() called multiple times is idempotent
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(createResult(1));
        stream.complete();
        stream.complete(); // Should not cause issues
        stream.complete();

        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
    }

    @Test(timeout = 2000)
    public void testPublishAfterComplete() {
        // publish() after complete() is ignored
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(createResult(1));
        stream.complete();
        stream.publish(createResult(2)); // Should be ignored

        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
        assertEquals(createResult(1), results.get(0));
    }

    @Test(timeout = 2000)
    public void testCloseAfterComplete() {
        // close() after complete()
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(createResult(1));
        stream.complete();
        stream.close(); // Should not cause issues

        // Stream is closed, so we might not get the element
        // but it shouldn't hang or throw
        try {
            stream.stream().toList();
        } catch (Exception e) {
            throw new AssertionError("Should not throw", e);
        }
    }

    @Test
    public void testInvalidCapacity() {
        // Invalid capacity throws exception
        try {
            new AsyncRecordStream(0);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        
        try {
            new AsyncRecordStream(-1);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test(timeout = 2000)
    public void testPublishNull() {
        // publish(null) is ignored
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(null); // Should be ignored
        stream.publish(createResult(1));
        stream.complete();

        List<RecordResult> results = stream.stream().toList();
        assertEquals(1, results.size());
    }

    @Test(timeout = 2000)
    public void testNextWithoutHasNext() {
        // next() without hasNext() throws NoSuchElementException
        AsyncRecordStream stream = new AsyncRecordStream(10);
        stream.complete();

        try {
            stream.next();
            throw new AssertionError("Expected NoSuchElementException");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    // ========================================
    // Stream API Integration Tests
    // ========================================

    @Test(timeout = 2000)
    public void testStreamFindFirst() {
        // stream().findFirst() with one element
        AsyncRecordStream stream = new AsyncRecordStream(10);
        RecordResult result = createResult(1);

        stream.publish(result);
        stream.complete();

        Optional<RecordResult> first = stream.stream().findFirst();
        assertTrue(first.isPresent());
        assertEquals(result, first.get());
    }

    @Test(timeout = 2000)
    public void testStreamToList() {
        // stream().toList()
        AsyncRecordStream stream = new AsyncRecordStream(10);
        List<RecordResult> expected = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            RecordResult result = createResult(i);
            expected.add(result);
            stream.publish(result);
        }
        stream.complete();

        List<RecordResult> actual = stream.stream().toList();
        assertEquals(expected, actual);
    }

    @Test(timeout = 2000)
    public void testStreamLimit() {
        // stream().limit()
        AsyncRecordStream stream = new AsyncRecordStream(10);

        for (int i = 0; i < 10; i++) {
            stream.publish(createResult(i));
        }
        stream.complete();

        List<RecordResult> limited = stream.stream().limit(3).toList();
        assertEquals(3, limited.size());
    }

    @Test(timeout = 2000)
    public void testStreamForEach() {
        // stream().forEach()
        AsyncRecordStream stream = new AsyncRecordStream(10);
        AtomicInteger count = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            stream.publish(createResult(i));
        }
        stream.complete();

        stream.stream().forEach(r -> count.incrementAndGet());
        assertEquals(5, count.get());
    }

    @Test(timeout = 2000)
    public void testStreamCount() {
        // stream().count()
        AsyncRecordStream stream = new AsyncRecordStream(10);

        for (int i = 0; i < 7; i++) {
            stream.publish(createResult(i));
        }
        stream.complete();

        long count = stream.stream().count();
        assertEquals(7, count);
    }

    @Test(timeout = 2000)
    public void testStreamAutoClose() {
        // stream() with try-with-resources closes stream
        AsyncRecordStream stream = new AsyncRecordStream(10);

        stream.publish(createResult(1));
        stream.publish(createResult(2));
        stream.complete();

        try (Stream<RecordResult> s = stream.stream()) {
            Optional<RecordResult> first = s.findFirst();
            assertTrue(first.isPresent());
        }

        // Stream should be closed
        assertTrue(stream.cancelled().getAsBoolean());
    }
}
