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
package com.aerospike.client.sdk.query.plan;

/**
 * Experimental process-wide switches for the query-plan evaluation (qopt) branches.
 *
 * <p>Initialized from system properties so a benchmark can select a mode without code changes,
 * and settable at runtime so one process can run several modes back to back.</p>
 *
 * <ul>
 *   <li>{@code aerospike.qopt.planMode} - {@link Mode} name (default {@code TWO_PHASE})</li>
 *   <li>{@code aerospike.qopt.planCacheTtlMs} - plan cache entry lifetime (default 60000)</li>
 *   <li>{@code aerospike.qopt.planCacheSize} - plan cache capacity (default 10000)</li>
 *   <li>{@code aerospike.qopt.genPollMs} - sindex generation poll period (default 1000)</li>
 * </ul>
 */
public final class QueryPlanSettings {
    public enum Mode {
        /** Shipped flow: explain on one node, then execute with the returned pins. */
        TWO_PHASE,
        /** A1: two-phase on a miss, execute-only with a cached plan on a hit. */
        CACHE,
        /** A2: single phase - every node plans inline (AUTO_PLAN), continuation pins the plan. */
        INLINE,
        /** A4: single phase with the plan pinned per partition, so nodes may disagree. */
        INLINE_PLANIDS,
        /** A5: cached plan pinned on a hit, A4 inline planning on a miss. */
        AUTO
    }

    private static volatile Mode mode = Mode.valueOf(
        System.getProperty("aerospike.qopt.planMode", Mode.TWO_PHASE.name()));
    private static volatile long cacheTtlMs = Long.getLong("aerospike.qopt.planCacheTtlMs", 60_000L);
    private static volatile int cacheSize = Integer.getInteger("aerospike.qopt.planCacheSize", 10_000);
    private static volatile long genPollMs = Long.getLong("aerospike.qopt.genPollMs", 1_000L);

    private QueryPlanSettings() {
    }

    public static Mode getMode() {
        return mode;
    }

    public static void setMode(Mode m) {
        mode = m;
    }

    public static boolean cacheEnabled() {
        return mode == Mode.CACHE;
    }

    public static boolean inlineEnabled() {
        return mode == Mode.INLINE || mode == Mode.INLINE_PLANIDS || mode == Mode.AUTO;
    }

    public static boolean perPartitionPlans() {
        return mode == Mode.INLINE_PLANIDS || mode == Mode.AUTO;
    }

    public static boolean autoEnabled() {
        return mode == Mode.AUTO;
    }

    public static long getCacheTtlMs() {
        return cacheTtlMs;
    }

    public static void setCacheTtlMs(long ms) {
        cacheTtlMs = ms;
    }

    public static int getCacheSize() {
        return cacheSize;
    }

    public static void setCacheSize(int size) {
        cacheSize = size;
    }

    public static long getGenPollMs() {
        return genPollMs;
    }

    public static void setGenPollMs(long ms) {
        genPollMs = ms;
    }
}
