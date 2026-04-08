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
import com.aerospike.client.sdk.Operation;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;

public final class BatchRead extends BatchRecord {
	public final List<Operation> ops;
	public final String[] binNames;
	public final int ttl;
	public final boolean readAllBins;

	public BatchRead(Key key, Expression where, BatchAttr attr, int ttl, List<Operation> ops) {
		super(key, where, attr);
		this.ops = ops;
		this.binNames = null;
		this.ttl = ttl;
		this.readAllBins = false;

		for (Operation op : ops) {
			if (op.type == Operation.Type.READ) {
				if (op.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
			}
			else if (op.type == Operation.Type.READ_HEADER) {
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}
	}

	public BatchRead(Key key, Expression where, BatchAttr attr, int ttl, String[] binNames) {
		super(key, where, attr);
		this.ops = null;
		this.binNames = binNames;
		this.ttl = ttl;

		if (binNames.length > 0) {
			this.readAllBins = false;
		}
		else {
			this.readAllBins = true;
			readAttr |= Command.INFO1_GET_ALL;
		}
	}

	public BatchRead(Key key, Expression where, BatchAttr attr, int ttl, boolean readAllBins) {
		super(key, where, attr);
		this.ops = null;
		this.binNames = null;
		this.ttl = ttl;
		this.readAllBins = readAllBins;

		if (readAllBins) {
			readAttr |= Command.INFO1_GET_ALL;
		}
		else {
			readAttr |= Command.INFO1_NOBINDATA;
		}
	}

	@Override
	public Type getType() {
		return Type.BATCH_READ;
	}

	@Override
	public boolean equals(BatchRecord obj) {
		if (getClass() != obj.getClass()) {
			return false;
		}

		BatchRead other = (BatchRead)obj;

		if (where != other.where) {
			return false;
		}

		return binNames == other.binNames && ops == other.ops && readAllBins == other.readAllBins;
	}

	@Override
	public int size(Command cmd) {
		int size = 0;

		if (where != null) {
			size += where.getBytes().length + Command.FIELD_HEADER_SIZE;
		}

		if (binNames != null) {
			for (String binName : binNames) {
				size += Buffer.estimateSizeUtf8(binName) + Command.OPERATION_HEADER_SIZE;
			}
		}
		else if (ops != null) {
			for (Operation op : ops) {
				if (op.type.isWrite) {
					throw AerospikeException.resultCodeToException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in batch read");
				}
				size += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
				size += op.value.estimateSize();
			}
		}
		return size;
	}
}
