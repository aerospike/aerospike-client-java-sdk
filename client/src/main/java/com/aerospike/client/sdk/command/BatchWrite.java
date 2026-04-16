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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.OpType;
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.OperationSpec;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;

public final class BatchWrite extends BatchRecord {
    public final OpType opType;
    public final List<Operation> ops;
    public final int gen;
    public final int ttl;

    public BatchWrite(
        Key key, Expression where, BatchAttr attr, OpType opType, List<Operation> ops,
        int gen, int ttl
    ) {
        super(key, where, attr);
        this.opType = opType;
        this.ops = ops;
        this.gen = gen;
        this.ttl = ttl;
        adjustWrite();
    }

    public BatchWrite(Key key, BatchAttr attr, OperationSpec spec, List<Operation> ops, OpType opType) {
        super(key, spec.getWhereClause(), attr);
        this.opType = opType;
        this.ops = ops;
        this.gen = spec.getGeneration();
        this.ttl = (int)spec.getExpirationInSeconds();
        adjustWrite();
    }

    public BatchWrite(Key key, BatchAttr attr, OperationSpec spec) {
        super(key, spec.getWhereClause(), attr);
        this.opType = spec.getOpType();
        this.ops = spec.getOperations();
        this.gen = spec.getGeneration();
        this.ttl = (int)spec.getExpirationInSeconds();
        adjustWrite();
    }

    /**
     * This constructor should only be used for transaction roll forward.
     */
    public BatchWrite(Key key, Expression where, BatchAttr attr, OpType opType) {
        super(key, where, attr);
        this.opType = opType;
        this.ops = null;
        this.gen = 0;
        this.ttl = 0;
    }

    private void adjustWrite() {
        if (gen > 0) {
            writeAttr |= Command.INFO2_GENERATION;
        }

        for (Operation op : ops) {
            if (! op.type.isWrite) {
                readAttr |= Command.INFO1_READ;

                if (op.type == Operation.Type.READ) {
                    if (op.binName == null) {
                        readAttr |= Command.INFO1_GET_ALL;
                        // When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
                        writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
                    }
                }
                else if (op.type == Operation.Type.READ_HEADER) {
                    readAttr |= Command.INFO1_NOBINDATA;
                }
            }
        }
    }

    @Override
    public Type getType() {
        return Type.BATCH_WRITE;
    }

    @Override
    public boolean equals(BatchRecord obj) {
        if (getClass() != obj.getClass()) {
            return false;
        }

        BatchWrite other = (BatchWrite)obj;

        if (opType != other.opType) {
            return false;
        }

        if (ops != other.ops) {
            return false;
        }

        if (where != other.where) {
            return false;
        }

        return gen == other.gen;
    }

    @Override
    public int size(Command cmd) {
        int size = 2; // gen(2) = 2

        if (cmd.sendKey) {
            size += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
        }

        if (ops != null) {
            boolean hasWrite = false;

            for (Operation op : ops) {
                if (op.type.isWrite) {
                    hasWrite = true;
                }
                size += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
                size += op.value.estimateSize();
            }

            if (! hasWrite) {
                throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR, "Batch write operations do not contain a write");
            }
        }

        if (where != null) {
            size += where.getBytes().length + Command.FIELD_HEADER_SIZE;
        }

        return size;
    }
}
