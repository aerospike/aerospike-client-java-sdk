/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Settings;

public class OperateWriteCommand extends WriteCommand {
	final Operation[] ops;
	final int size;
	final int readAttr;
	final int writeAttr;
	final boolean hasWrite;

	public OperateWriteCommand(
		Cluster cluster, Partitions partitions, Txn txn, Key key, Operation[] ops, OpType type,
		int gen, int ttl, Expression filterExp, boolean failOnFilteredOut, Settings policy
	) {
		super(cluster, partitions, txn, key, type, gen, ttl, filterExp, failOnFilteredOut, policy);
		this.ops = ops;

		int dataOffset = 0;
		int rattr = 0;
		int wattr = 0;
		boolean write = false;
		boolean readBin = false;
		boolean readHeader = false;

		for (Operation op : ops) {
			switch (op.type) {
			case BIT_READ:
			case EXP_READ:
			case HLL_READ:
			case MAP_READ:
			case CDT_READ:
			case READ:
				rattr |= Command.INFO1_READ;

				// Read all bins if no bin is specified.
				if (op.binName == null) {
					rattr |= Command.INFO1_GET_ALL;
				}
				readBin = true;
				break;

			case READ_HEADER:
				rattr |= Command.INFO1_READ;
				readHeader = true;
				break;

			case BIT_MODIFY:
			case EXP_MODIFY:
			case HLL_MODIFY:
			case MAP_MODIFY:
			default:
				wattr = Command.INFO2_WRITE;
				write = true;
				break;
			}
			dataOffset += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
			dataOffset += op.value.estimateSize();
		}
		size = dataOffset;
		hasWrite = write;

		if (readHeader && ! readBin) {
			rattr |= Command.INFO1_NOBINDATA;
		}
		readAttr = rattr;

		// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
		if ((rattr & Command.INFO1_GET_ALL) == 0) {
			wattr |= Command.INFO2_RESPOND_ALL_OPS;
		}
		writeAttr = wattr;
	}

	public boolean hasWrite() {
		return hasWrite;
	}
}
