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

import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;

/**
 * Wires {@link TopKMerge} into the query execution path: drains the raw per-node results,
 * merges/dedupes/truncates them to the global Top-K, and re-publishes the result as an ordinary
 * {@link RecordStream}.
 *
 * <p>Draining uses {@link AsyncRecordStream#asCompletableFuture()} rather than a dedicated
 * consumer thread: {@code source} is already completed from a cluster-managed virtual thread
 * (see {@code QueryCommand.execute(...)}), so the merge just runs as a continuation on that same
 * thread instead of spinning up another one to wait on it.</p>
 */
final class TopKMergingRecordStream {

    private TopKMergingRecordStream() {
    }

    /**
     * @param source the raw {@link AsyncRecordStream} that {@code QueryCommand.execute(...)}
     *               publishes into; ownership transfers here
     * @param spec   the order-by clause (drives key extraction, comparator direction)
     * @param k      the Top-K limit
     * @return a {@link RecordStream} yielding at most {@code k} merged/deduped results, plus
     *         any errors encountered along the way
     */
    static RecordStream merge(AsyncRecordStream source, OrderBySpec spec, int k) {
        AsyncRecordStream merged = new AsyncRecordStream(Math.max(100, k));

        source.asCompletableFuture().whenComplete((buffer, ex) -> {
            if (ex != null) {
                merged.error(ex);
                return;
            }
            try {
                for (RecordResult rr : TopKMerge.mergeSortDedupeTruncate(buffer, spec, k)) {
                    merged.publish(rr);
                }
                merged.complete();
            }
            catch (Throwable t) {
                merged.error(t);
            }
        });

        return new RecordStream(merged);
    }
}
