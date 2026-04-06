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
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.util.Packer;

public final class BatchUDF extends BatchRecord {
	public final String packageName;
	public final String functionName;
	public final Value[] functionArgs;
	public byte[] argBytes;
	public final int ttl;

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

	@Override
	public Type getType() {
		return Type.BATCH_UDF;
	}

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
