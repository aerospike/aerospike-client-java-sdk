/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.fluent;

import com.aerospike.client.fluent.command.QueryCommand;
import com.aerospike.client.fluent.query.RecordStreamImpl;

/**
 * A RecordStreamImpl that supports server-side streaming with chunking.
 *
 * <p>This implementation fetches records in chunks from the server, allowing for
 * efficient processing of large result sets without loading all data into memory.
 * The chunks are fetched on-demand as the client iterates through the results.</p>
 *
 * <p>This is distinct from client-side pagination provided by {@link NavigatableRecordStream},
 * which loads all data into memory and provides bi-directional navigation and sorting.</p>
 *
 * <p><b>Key characteristics:</b></p>
 * <ul>
 *   <li>Forward-only iteration</li>
 *   <li>No sorting capability</li>
 *   <li>Suitable for billions of records</li>
 *   <li>Low memory footprint</li>
 * </ul>
 */
public class ChunkedRecordStream implements RecordStreamImpl {
	private final QueryCommand cmd;
    private final long limit;
    private long recordCount = 0;
    private AsyncRecordStream stream;
    private final int recordQueueSize;
    private boolean first = true;

    public ChunkedRecordStream(AsyncRecordStream stream, QueryCommand cmd, long limit, int recordQueueSize) {
        this.cmd = cmd;
        this.limit = limit;
    	this.recordQueueSize = recordQueueSize;
    	this.stream = stream;
    }

    @Override
    public boolean hasMoreChunks() {
    	if (first) {
    		first = false;
    		return true;
    	}

        if (cmd.isDone() || (limit > 0 && recordCount >= limit)) {
            return false;
        }

        // Query next chunk.
    	stream = new AsyncRecordStream(recordQueueSize);
    	cmd.execute(stream);
		return true;
    }

    @Override
    public boolean hasNext() {
        if (limit > 0 && recordCount >= limit) {
            return false;
        }

        return stream.hasNext();
    }

    @Override
    public RecordResult next() {
        recordCount++;
        return stream.next();
    }

    @Override
    public void close() {
    	stream.close();
    }
}
