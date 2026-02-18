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

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.util.Packer;

/**
 * Batch user defined functions.
 */
public final class BatchUDF extends BatchRecord {
	/**
	 * Package or lua module name.
	 */
	public final String packageName;

	/**
	 * Lua function name.
	 */
	public final String functionName;

	/**
	 * Optional arguments to lua function.
	 */
	public final Value[] functionArgs;

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
	 * Wire protocol bytes for function args. For internal use only.
	 */
	public byte[] argBytes;

	/**
	 * Constructor using default policy.
	 */
	public BatchUDF(
		Key key, Expression where, BatchAttr attr, String packageName, String functionName,
		Value[] functionArgs, int ttl
	) {
		super(key, where, attr);
		this.packageName = packageName;
		this.functionName = functionName;
		this.functionArgs = functionArgs;
		this.ttl = ttl;
		// Do not set argBytes here because may not be necessary if batch repeat flag is used.
	}

	/**
	 * Return batch command type.
	 */
	@Override
	public Type getType() {
		return Type.BATCH_UDF;
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

		BatchUDF other = (BatchUDF)obj;

		if (where != other.where) {
			return false;
		}

		if (functionName != other.functionName || functionArgs != other.functionArgs ||
				packageName != other.packageName || ttl != other.ttl) {
			return false;
		}

		return true;
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

		if (where != null) {
			size += where.getBytes().length + Command.FIELD_HEADER_SIZE;
		}

		size += Buffer.estimateSizeUtf8(packageName) + Command.FIELD_HEADER_SIZE;
		size += Buffer.estimateSizeUtf8(functionName) + Command.FIELD_HEADER_SIZE;
		argBytes = Packer.pack(functionArgs);
		size += argBytes.length + Command.FIELD_HEADER_SIZE;
		return size;
	}
}
