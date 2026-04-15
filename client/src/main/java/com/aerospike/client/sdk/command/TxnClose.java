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
package com.aerospike.client.sdk.command;

import java.io.IOException;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.metrics.LatencyType;

public final class TxnClose extends SyncExecutor {
    private final WriteCommand write;

    public TxnClose(Cluster cluster, WriteCommand cmd) {
        super(cluster, cmd);
        this.write = cmd;
    }

    @Override
    protected final boolean isWrite() {
        return true;
    }

    @Override
    protected final Node getNode() {
        return write.partition.getNodeWrite(cluster);
    }

    @Override
    protected final LatencyType getLatencyType() {
        return LatencyType.WRITE;
    }

    @Override
    protected CommandBuffer getCommandBuffer() {
        CommandBuffer cb = new CommandBuffer();
        cb.setTxnClose(write);
        return cb;
    }

    @Override
    protected void parseResult(Node node, Connection conn, byte[] buffer) throws IOException {
        RecordParser rp = new RecordParser(conn, buffer);
        rp.skipFields();

        if (node.isMetricsEnabled()) {
            node.addBytesIn(cmd.namespace, rp.bytesIn);
        }

        if (rp.resultCode == ResultCode.OK || rp.resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            return;
        }

        throw AerospikeException.resultCodeToException(rp.resultCode, null);
    }

    @Override
    protected final boolean prepareRetry(boolean timeout) {
        write.partition.prepareRetryWrite(timeout);
        return true;
    }

    @Override
    protected void onInDoubt() {
    }
}
