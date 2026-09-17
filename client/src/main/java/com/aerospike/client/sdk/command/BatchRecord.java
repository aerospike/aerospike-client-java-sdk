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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ExpressionTrace;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.util.ContainerString;

/**
 * Batch key and record result.
 */
public class BatchRecord {
    public final Key key;
    public final Expression where;
    public Node node;
    public Record record;
    public String message;
    public ExpressionTrace expTrace;
    public int resultCode;
    public int subCode;
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
    public final void setError(AerospikeException ae, boolean inDoubt) {
        this.resultCode = ae.getResultCode();
        this.subCode = ae.getSubCode();
        this.message = ae.getMessage();
        this.expTrace = ae.getExpressionTrace();
        this.inDoubt = inDoubt;
    }

    /**
     * Set error result. For internal use only.
     */
    public final void setError(RecordParser rp, boolean inDoubt) {
        this.resultCode = rp.resultCode;
        this.subCode = rp.subCode;
        this.message = rp.message;
        this.expTrace = rp.expTrace;
        this.inDoubt = inDoubt;
    }

    /**
     * Set error result. For internal use only.
     */
    public final void setErrorUDF(RecordParser rp, boolean inDoubt) {
        this.resultCode = rp.resultCode;
        this.subCode = rp.subCode;
        this.expTrace = rp.expTrace;
        this.inDoubt = inDoubt;

        Record r = rp.parseRecord(false);
        String m = r.getString("FAILURE");

        if (m != null) {
            this.message = m;
        }
        else {
            this.message = rp.message;
        }
    }

    /**
     * Convert batch response to an exception.
     */
    public AerospikeException toException() {
        return AerospikeException.toException(resultCode, subCode, message, expTrace, inDoubt);
    }

    /**
     * Convert to string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(200);
        sb.append("BatchRecord{type=");
        sb.append(getType());
        sb.append(", key=");
        sb.append(key);
        sb.append(", resultCode=");
        sb.append(resultCode);
        sb.append(", subCode=");
        sb.append(subCode);
        sb.append(", inDoubt=");
        sb.append(inDoubt);
        sb.append(", hasWrite=");
        sb.append(hasWrite);
        sb.append(", linearize=");
        sb.append(linearize);

        appendField(sb, "message", message);
        appendField(sb, "record", record);
        appendField(sb, "where", where);
        appendField(sb, "expTrace", expTrace);
        appendField(sb, "node", node);

        sb.append('}');
        return sb.toString();
    }

    private static void appendField(StringBuilder sb, String name, Object value) {
        if (value != null) {
            sb.append(", ");
            sb.append(name);
            sb.append('=');
            ContainerString.append(sb, value, ContainerString.DEFAULT_MAX_CHARS);
        }
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
