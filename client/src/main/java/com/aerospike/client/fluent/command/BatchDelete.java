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
import com.aerospike.client.fluent.OperationSpec;
import com.aerospike.client.fluent.exp.Expression;

public final class BatchDelete extends BatchRecord {
	public final int gen;

	public BatchDelete(Key key, Expression where, BatchAttr attr, int gen) {
		super(key, where, attr);
		this.gen = gen;

		if (gen > 0) {
			writeAttr |= Command.INFO2_GENERATION;
		}
	}

	public BatchDelete(Key key, BatchAttr attr, OperationSpec spec) {
		super(key, spec.getWhereClause(), attr);
		this.gen = (short)spec.getGeneration();

		if (gen > 0) {
			writeAttr |= Command.INFO2_GENERATION;
		}
	}

	@Override
	public Type getType() {
		return Type.BATCH_DELETE;
	}

	@Override
	public boolean equals(BatchRecord obj) {
		if (getClass() != obj.getClass()) {
			return false;
		}

		BatchDelete other = (BatchDelete)obj;

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

		if (where != null) {
			size += where.getBytes().length + Command.FIELD_HEADER_SIZE;
		}

		return size;
	}
}
