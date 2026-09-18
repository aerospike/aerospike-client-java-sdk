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
import java.util.List;

/** Applies {@link TopKMerge} to a query result stream. */
final class TopKMergingRecordStream {

    private TopKMergingRecordStream() {
    }

    /** Return the merged result stream. */
    static RecordStream merge(AsyncRecordStream source, List<OrderBySpec> specs, int k) {
        AsyncRecordStream merged = new AsyncRecordStream(Math.max(100, k));

        Thread.startVirtualThread(() -> {
            try {
                TopKMerge.Reducer reducer = new TopKMerge.Reducer(specs, k);
                while (source.hasNext()) {
                    // Stop when the output stream closes.
                    if (merged.cancelled().getAsBoolean()) {
                        return;
                    }
                    reducer.accept(source.next());
                }
                for (RecordResult rr : reducer.finish()) {
                    merged.publish(rr);
                }
                merged.complete();
            }
            catch (Throwable t) {
                merged.error(t);
            }
            finally {
                source.close();
            }
        });

        return new RecordStream(merged);
    }
}
