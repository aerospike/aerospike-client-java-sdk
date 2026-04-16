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

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;

/**
 * Batch key and record result.
 */
public class BatchRecord {
    public final Key key;
    public final Expression where;
    public Node node;
    public Record record;
    public int resultCode;
    public byte readAttr;
    public byte writeAttr;
    public final byte infoAttr;
    public final byte txnAttr;
    public final boolean hasWrite;
    public final boolean linearize;
    public boolean inDoubt;

    /**
     * Initialize batch key.
     */
    public BatchRecord(Key key, Expression where, BatchAttr attr) {
        this.key = key;
        this.where = where;
        this.readAttr = attr.readAttr;
        this.writeAttr = attr.writeAttr;
        this.infoAttr = attr.infoAttr;
        this.txnAttr = attr.txnAttr;
        this.resultCode = ResultCode.NO_RESPONSE;
        this.hasWrite = attr.hasWrite;
        this.linearize = attr.linearize;
    }

    /**
     * Initialize batch key.
     */
    public BatchRecord(Key key, boolean hasWrite) {
        this.key = key;
        this.where = null;
        this.readAttr = 0;
        this.writeAttr = 0;
        this.infoAttr = 0;
        this.txnAttr = 0;
        this.resultCode = ResultCode.NO_RESPONSE;
        this.hasWrite = hasWrite;
        this.linearize = false;
    }

    /**
     * Prepare for upcoming batch call. Reset result fields because this instance might be
     * reused. For internal use only.
     */
    public final void prepare() {
        this.record = null;
        this.resultCode = ResultCode.NO_RESPONSE;
        this.inDoubt = false;
    }

    /**
     * Set record result. For internal use only.
     */
    public final void setRecord(Record record) {
        this.record = record;
        this.resultCode = ResultCode.OK;
    }

    /**
     * Set error result. For internal use only.
     */
    public final void setError(int resultCode, boolean inDoubt) {
        this.resultCode = resultCode;
        this.inDoubt = inDoubt;
    }

    /**
     * Convert to string.
     */
    @Override
    public String toString() {
        return key.toString();
    }

    /**
     * Return batch command type. For internal use only.
     */
    public Type getType() {
        return null;
    }

    /**
     * Optimized reference equality check to determine batch wire protocol repeat flag.
     * For internal use only.
     */
    public boolean equals(BatchRecord other) {
        return false;
    }

    /**
     * Return wire protocol size. For internal use only.
     */
    public int size(Command cmd) {
        return 0;
    }

    /**
     * Batch command type.
     */
    public enum Type {
        BATCH_READ,
        BATCH_WRITE,
        BATCH_DELETE,
        BATCH_UDF
    }
}
