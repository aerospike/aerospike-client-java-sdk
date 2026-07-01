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

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.metrics.LatencyType;
import com.aerospike.client.sdk.policy.Replica;

public final class Batch {
    //-------------------------------------------------------
    // OperateList
    //-------------------------------------------------------

    public static final class OperateListSync extends BatchNodeExecutor {
        private final List<BatchRecord> records;

        public OperateListSync(
            Cluster cluster,
            BatchCommand parent,
            BatchNode batch,
            List<BatchRecord> records,
            BatchStatus status
        ) {
            super(cluster, parent, batch, status, true);
            this.records = records;
        }

        @Override
        protected boolean isWrite() {
            // This method is only called to set inDoubt on node level errors.
            // setError() will filter out reads when setting record level inDoubt.
            return true;
        }

        @Override
        protected CommandBuffer getCommandBuffer() {
            CommandBuffer cb = new CommandBuffer();
            cb.setBatchOperate(parent, batch);
            return cb;
        }

        @Override
        protected boolean parseRow() {
            BatchRecord br = records.get(parser.batchIndex);

            parser.parseFields(parent.txn, br.key, br.hasWrite);

            if (parser.resultCode == 0) {
                br.setRecord(parser.parseRecord(isOperation));
                return true;
            }

            if (parser.resultCode == ResultCode.UDF_BAD_RESPONSE) {
                br.setErrorUDF(parser, BatchCommand.inDoubt(br.hasWrite, commandSentCounter));
                status.setRowError();
                return true;
            }

            br.setError(parser, BatchCommand.inDoubt(br.hasWrite, commandSentCounter));
            status.setRowError();
            return true;
        }

        @Override
        protected void setException(AerospikeException ae) {
            for (int index : batch.offsets) {
                BatchRecord br = records.get(index);

                if (br.resultCode == ResultCode.NO_RESPONSE) {
                    br.resultCode = ae.getResultCode();
                    br.inDoubt = ae.getInDoubt();

                    if (br.inDoubt && parent.txn != null) {
                        parent.txn.onWriteInDoubt(br.key);
                    }
                }
            }
        }

        @Override
        protected BatchNodeExecutor createCommand(BatchNode batchNode) {
            return new OperateListSync(cluster, parent, batchNode, records, status);
        }

        @Override
        protected List<BatchNode> generateBatchNodes() {
            return BatchNodes.generate(cluster, parent, records, sequenceAP, sequenceSC, batch,
                status);
        }
    }

    public static final class OperateListAsync extends BatchNodeExecutor {
        private final List<BatchRecord> records;
        private final AsyncRecordStream stream;

        public OperateListAsync(
            Cluster cluster,
            BatchCommand parent,
            BatchNode batch,
            List<BatchRecord> records,
            AsyncRecordStream stream,
            BatchStatus status
        ) {
            super(cluster, parent, batch, status, true);
            this.records = records;
            this.stream = stream;
        }

        @Override
        protected boolean isWrite() {
            // This method is only called to set inDoubt on node level errors.
            // setError() will filter out reads when setting record level inDoubt.
            return true;
        }

        @Override
        protected CommandBuffer getCommandBuffer() {
            CommandBuffer cb = new CommandBuffer();
            cb.setBatchOperate(parent, batch);
            return cb;
        }

        @Override
        protected boolean parseRow() {
            BatchRecord br = records.get(parser.batchIndex);

            parser.parseFields(parent.txn, br.key, br.hasWrite);

            if (parser.resultCode == 0) {
                Record rec = parser.parseRecord(isOperation);

                br.setRecord(rec);

                if (br.hasWrite || parent.includeMissingKeys || rec != null) {
                    stream.publish(new RecordResult(br, parser.batchIndex));
                }
                return true;
            }

            if (parser.resultCode == ResultCode.UDF_BAD_RESPONSE) {
                br.setErrorUDF(parser, BatchCommand.inDoubt(br.hasWrite, commandSentCounter));
                status.setRowError();
                stream.publish(new RecordResult(br, parser.batchIndex));
                return true;
            }

            br.setError(parser, BatchCommand.inDoubt(br.hasWrite, commandSentCounter));
            status.setRowError();

            boolean shouldPublish = switch (parser.resultCode) {
                case ResultCode.FILTERED_OUT -> br.hasWrite || parent.failOnFilteredOut;
                case ResultCode.KEY_NOT_FOUND_ERROR -> br.hasWrite || parent.includeMissingKeys;
                default -> true;
            };

            if (shouldPublish) {
                stream.publish(new RecordResult(br, parser.batchIndex));
            }
            return true;
        }

        @Override
        protected void setException(AerospikeException ae) {
            for (int index : batch.offsets) {
                BatchRecord br = records.get(index);

                if (br.resultCode == ResultCode.NO_RESPONSE) {
                    br.resultCode = ae.getResultCode();
                    br.inDoubt = ae.getInDoubt();

                    if (br.inDoubt && parent.txn != null) {
                        parent.txn.onWriteInDoubt(br.key);
                    }

                    stream.publish(new RecordResult(br, ae, parser.batchIndex));
                }
            }
        }

        @Override
        protected BatchNodeExecutor createCommand(BatchNode batchNode) {
            return new OperateListSync(cluster, parent, batchNode, records, status);
        }

        @Override
        protected List<BatchNode> generateBatchNodes() {
            return BatchNodes.generate(cluster, parent, records, sequenceAP, sequenceSC, batch,
                status);
        }
    }

    //-------------------------------------------------------
    // Transaction
    //-------------------------------------------------------

    public static final class TxnVerify extends BatchNodeExecutor {
        private final List<BatchRecord> records;
        private final Long[] versions;

        public TxnVerify(
            Cluster cluster,
            BatchCommand parent,
            BatchNode batch,
            BatchStatus status,
            Long[] versions
        ) {
            super(cluster, parent, batch, status, false);
            this.records = parent.records;
            this.versions = versions;
        }

        @Override
        protected boolean isWrite() {
            return false;
        }

        @Override
        protected CommandBuffer getCommandBuffer() {
            CommandBuffer cb = new CommandBuffer();
            cb.setBatchOperate(parent, batch);
            return cb;
        }

        @Override
        protected boolean parseRow() {
            parser.parseFieldsError();

            BatchRecord br = records.get(parser.batchIndex);

            if (parser.resultCode == ResultCode.OK) {
                br.resultCode = parser.resultCode;
            }
            else {
                br.setError(parser, false);
                status.setRowError();
            }
            return true;
        }

        @Override
        protected void setException(AerospikeException ae) {
            for (int index : batch.offsets) {
                BatchRecord br = records.get(index);

                if (br.resultCode == ResultCode.NO_RESPONSE) {
                    br.resultCode = ae.getResultCode();
                    br.inDoubt = ae.getInDoubt();
                }
            }
        }

        @Override
        protected TxnVerify createCommand(BatchNode batchNode) {
            return new TxnVerify(cluster, parent, batchNode, status, versions);
        }

        @Override
        protected List<BatchNode> generateBatchNodes() {
            return BatchNodes.generate(cluster, parent, records, sequenceAP, sequenceSC, batch,
                status);
        }
    }

    public static final class TxnRoll extends BatchNodeExecutor {
        private final List<BatchRecord> records;

        public TxnRoll(
            Cluster cluster,
            BatchCommand parent,
            BatchNode batch,
            BatchStatus status,
            List<BatchRecord> records
        ) {
            super(cluster, parent, batch, status, false);
            this.records = records;
        }

        @Override
        protected boolean isWrite() {
            return true;
        }

        @Override
        protected CommandBuffer getCommandBuffer() {
            CommandBuffer cb = new CommandBuffer();
            cb.setBatchTxnRoll(parent, batch);
            return cb;
        }

        @Override
        protected boolean parseRow() {
            parser.parseFieldsError();

            BatchRecord br = records.get(parser.batchIndex);

            if (parser.resultCode == 0) {
                br.resultCode = parser.resultCode;
            }
            else {
                br.setError(parser, BatchCommand.inDoubt(true, commandSentCounter));
                status.setRowError();
            }
            return true;
        }

        @Override
        protected void inDoubt() {
            for (int index : batch.offsets) {
                BatchRecord record = records.get(index);

                if (record.resultCode == ResultCode.NO_RESPONSE) {
                    record.inDoubt = true;
                }
            }
        }

        @Override
        protected void setException(AerospikeException ae) {
            for (int index : batch.offsets) {
                BatchRecord br = records.get(index);

                if (br.resultCode == ResultCode.NO_RESPONSE) {
                    br.resultCode = ae.getResultCode();
                    br.inDoubt = ae.getInDoubt();
                }
            }
        }

        @Override
        protected TxnRoll createCommand(BatchNode batchNode) {
            return new TxnRoll(cluster, parent, batchNode, status, records);
        }

        @Override
        protected List<BatchNode> generateBatchNodes() {
            return BatchNodes.generate(cluster, parent, records, sequenceAP, sequenceSC, batch,
                status);
        }
    }

    //-------------------------------------------------------
    // Batch Base Command
    //-------------------------------------------------------

    public static abstract class BatchNodeExecutor extends NodeExecutor implements IBatchCommand {
        final BatchCommand parent;
        final BatchNode batch;
        final BatchStatus status;
        int sequenceAP;
        int sequenceSC;
        boolean splitRetry;

        public BatchNodeExecutor(
            Cluster cluster,
            BatchCommand parent,
            BatchNode batch,
            BatchStatus status,
            boolean isOperation
        ) {
            super(cluster, parent, batch.node, isOperation);
            this.parent = parent;
            this.batch = batch;
            this.status = status;
        }

        @Override
        public void run() {
            try {
                if (!splitRetry) {
                    execute();
                }
                else {
                    executeCommand();
                }
            }
            catch (AerospikeException ae) {
                setException(ae);
                status.setException(ae);
            }
            catch (Throwable e) {
                AerospikeException ae = new AerospikeException(e);
                ae.setInDoubt(true);
                setException(ae);
                status.setException(new AerospikeException(e));
            }
        }

        @Override
        protected void addSubException(AerospikeException ae) {
            status.addSubException(ae);
        }

        @Override
        protected LatencyType getLatencyType() {
            return LatencyType.BATCH;
        }

        @Override
        protected boolean prepareRetry(boolean timeout) {
            if (! (parent.replica == Replica.SEQUENCE || parent.replica == Replica.PREFER_RACK)) {
                // Perform regular retry to same node.
                return true;
            }
            sequenceAP++;

            if (! timeout || !parent.linearize) {
                sequenceSC++;
            }
            return false;
        }

        @Override
        protected boolean retryBatch(
            Cluster cluster,
            int socketTimeout,
            int totalTimeout,
            long deadline,
            int iteration,
            int commandSentCounter
        ) {
            // Retry requires keys for this node to be split among other nodes.
            // This is both recursive and exponential.
            List<BatchNode> batchNodes = generateBatchNodes();

            if (batchNodes.size() == 1 && batchNodes.get(0).node == batch.node) {
                // Batch node is the same.  Go through normal retry.
                return false;
            }

            splitRetry = true;

            // Run batch retries in parallel using virtual threads.
            try (ExecutorService es = cluster.getExecutorService();) {
                for (BatchNode batchNode : batchNodes) {
                    BatchNodeExecutor exec = createCommand(batchNode);
                    exec.sequenceAP = sequenceAP;
                    exec.sequenceSC = sequenceSC;
                    exec.socketTimeout = socketTimeout;
                    exec.totalTimeout = totalTimeout;
                    exec.iteration = iteration;
                    exec.commandSentCounter = commandSentCounter;
                    exec.deadline = deadline;

                    cluster.addRetry();
                    es.execute(exec);
                }
            }
            return true;
        }

        @Override
        public void setInDoubt() {
            // Set error/inDoubt for keys associated this batch command when
            // the command was not retried and split. If a split retry occurred,
            // those new subcommands have already set inDoubt on the affected
            // subset of keys.
            if (! splitRetry) {
                inDoubt();
            }
        }

        protected void inDoubt() {
            // Do nothing by default. Batch writes will override this method.
        }

        abstract BatchNodeExecutor createCommand(BatchNode batchNode);
        abstract List<BatchNode> generateBatchNodes();
        abstract void setException(AerospikeException ae);
    }
}
