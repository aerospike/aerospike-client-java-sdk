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
package com.aerospike.client.fluent.command;

import java.util.List;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.exp.Expression;

/**
 * Batch key and read/write operations with write policy.
 */
public final class BatchWrite extends BatchRecord {
	/**
	 * Optional expression filter. If filterExp exists and evaluates to false, the specific batch key
	 * request is not performed and the result code is set to
	 * {@link com.aerospike.client.fluent.ResultCode#FILTERED_OUT}.
	 * <p>
	 * If exists, this filter overrides the batch parent filter expression.
	 * <p>
	 * Default: null
	 */
	public final Expression filterExp;

	/**
	 * Required operations for this key.
	 */
	public final List<Operation> ops;

	/**
	 * Write operation type.
	 */
	public final OpType opType;

	/**
	 * Expected generation. Generation is the number of times a record has been modified
	 * (including creation) on the server. If a write operation is creating a record,
	 * the expected generation would be <code>0</code>. This field is only relevant when
	 * generationPolicy is not NONE.
	 * <p>
	 * The server does not support this field for UDF execute() calls. The read-modify-write
	 * usage model can still be enforced inside the UDF code itself.
	 * <p>
	 * Default: 0
	 */
	public final int generation;

	/**
	 * Record expiration. Also known as ttl (time to live).
	 * Seconds record will live before being removed by the server.
	 * <p>
	 * Expiration values:
	 * <ul>
	 * <li>-2: Do not change ttl when record is updated.</li>
	 * <li>-1: Never expire.</li>
	 * <li>0: Default to namespace configuration variable "default-ttl" on the server.</li>
	 * <li>&gt; 0: Actual ttl in seconds.<br></li>
	 * </ul>
	 * <p>
	 * Default: 0
	 */
	public final int ttl;

	/**
	 * Initialize batch key and read/write operations.
	 * <p>
	 * {@link Operation#get()} is not allowed because it returns a variable number of bins and
	 * makes it difficult (sometimes impossible) to lineup operations with results. Instead,
	 * use {@link Operation#get(String)} for each bin name.
	 */
	public BatchWrite(Key key, List<Operation> ops) {
		super(key, true);
		this.filterExp = null;
		this.ops = ops;
		this.opType = OpType.UPSERT;
		this.generation = 0;
		this.ttl = 0;
	}

	/**
	 * Initialize policy, batch key and read/write operations.
	 * <p>
	 * {@link Operation#get()} is not allowed because it returns a variable number of bins and
	 * makes it difficult (sometimes impossible) to lineup operations with results. Instead,
	 * use {@link Operation#get(String)} for each bin name.
	 */
	public BatchWrite(
		Key key, Expression filterExp, List<Operation> ops, OpType opType, int generation, int ttl
	) {
		super(key, true);
		this.filterExp = filterExp;
		this.ops = ops;
		this.opType = opType;
		this.generation = generation;
		this.ttl = ttl;
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_WRITE;
	}

	/**
	 * Optimized reference equality check to determine batch wire protocol repeat flag.
	 * For internal use only.
	 */
	@Override
	public boolean equals(BatchRecord obj) {
		if (getClass() != obj.getClass()) {
			return false;
		}

		BatchWrite other = (BatchWrite)obj;

		if (ops != other.ops) {
			return false;
		}

		if (opType != other.opType) {
			return false;
		}

		if (filterExp != other.filterExp) {
			return false;
		}

		return generation == other.generation;
	}

	/**
	 * Return wire protocol size. For internal use only.
	 */
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
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Batch write operations do not contain a write");
			}
		}

		if (filterExp != null) {
			size += filterExp.getBytes().length + Command.FIELD_HEADER_SIZE;
		}

		return size;
	}
}
