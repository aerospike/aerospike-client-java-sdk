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

import java.util.List;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.Settings;

public class BatchReadCommand extends BatchCommand {
    final ReadModeAP readModeAP;
    final ReadModeSC readModeSC;
    final int readTouchTtlPercent;
    final boolean linearize;

	private BatchReadCommand(
		Cluster cluster, Partitions partitions, Txn txn, String namespace,
		List<BatchRecord> records, Expression filterExp, Replica replica, ReadModeAP mode,
		boolean respondAllKeys, Settings policy
	) {
		super(cluster, partitions, txn, namespace, records, filterExp, replica, respondAllKeys,
			policy);

		this.readModeAP = mode;
		this.readModeSC = ReadModeSC.SESSION;
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
        this.linearize = false;
	}

	private BatchReadCommand(
		Cluster cluster, Partitions partitions, Txn txn, String namespace,
		List<BatchRecord> records, Expression filterExp, Replica replica, ReadModeSC mode,
		boolean respondAllKeys, boolean linearize, Settings policy
	) {
		super(cluster, partitions, txn, namespace, records, filterExp, replica, respondAllKeys,
			policy);
		this.readModeAP = ReadModeAP.ONE;
		this.readModeSC = mode;
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
        this.linearize = linearize;
	}

	public static BatchReadCommand create(
		Cluster cluster, Txn txn, String namespace, List<BatchRecord> recs, Partitions partitions,
		Expression filterExp, Settings policy, boolean respondAllKeys
	) {
		BatchReadCommand cmd;
		Replica replica;

        if (partitions.scMode) {
            ReadModeSC mode = policy.getReadModeSC();
            boolean linearize;

            switch (mode) {
            case SESSION:
            	replica = Replica.MASTER;
            	linearize = false;
                break;

            case LINEARIZE:
                replica = policy.getReplicaOrder();

                if (replica == Replica.PREFER_RACK) {
                    replica = Replica.SEQUENCE;
                }
            	linearize = true;
                break;

            default:
                replica = policy.getReplicaOrder();
            	linearize = false;
                break;
            }

            cmd = new BatchReadCommand(cluster, partitions, txn, namespace, recs, filterExp,
            	replica, mode, respondAllKeys, linearize, policy);
        }
        else {
            replica = policy.getReplicaOrder();
            cmd = new BatchReadCommand(cluster, partitions, txn, namespace, recs, filterExp,
            	replica, policy.getReadModeAP(), respondAllKeys, policy);
        }

        return cmd;
	}
}
