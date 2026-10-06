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

import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters for the query-plan evaluation (qopt) branches. A benchmark samples
 * {@link #snapshot()} before and after a query or trial and reports the difference.
 */
public final class QueryPlanStats {
    public static final LongAdder explains = new LongAdder();
    public static final LongAdder explainBytesOut = new LongAdder();
    public static final LongAdder executeCommands = new LongAdder();
    public static final LongAdder executeBytesOut = new LongAdder();
    public static final LongAdder cacheHits = new LongAdder();
    public static final LongAdder cacheMisses = new LongAdder();
    public static final LongAdder cacheInvalidations = new LongAdder();
    public static final LongAdder genPolls = new LongAdder();
    public static final LongAdder planHeaders = new LongAdder();
    public static final LongAdder planDisagreements = new LongAdder();
    public static final LongAdder filteredOutNodes = new LongAdder();

    private QueryPlanStats() {
    }

    public static long[] snapshot() {
        return new long[] {
            explains.sum(), explainBytesOut.sum(), executeCommands.sum(), executeBytesOut.sum(),
            cacheHits.sum(), cacheMisses.sum(), cacheInvalidations.sum(), genPolls.sum(),
            planHeaders.sum(), planDisagreements.sum(), filteredOutNodes.sum()
        };
    }

    public static String[] names() {
        return new String[] {
            "explains", "explainBytesOut", "executeCommands", "executeBytesOut",
            "cacheHits", "cacheMisses", "cacheInvalidations", "genPolls",
            "planHeaders", "planDisagreements", "filteredOutNodes"
        };
    }
}
