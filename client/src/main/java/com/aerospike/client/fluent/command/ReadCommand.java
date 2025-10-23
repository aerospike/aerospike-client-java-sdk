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
import com.aerospike.client.fluent.Partition;
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.Settings;

public class ReadCommand extends Command {
    final Key key;
    final Partition partition;
    final ReadModeAP readModeAP;
    final ReadModeSC readModeSC;
    final String[] binNames;
    final int readTouchTtlPercent;
    final boolean withNoBins;

    public ReadCommand(
            Cluster cluster, Partitions partitions, Txn txn, Key key, String[] binNames,
            boolean withNoBins, Expression filterExp, boolean failOnFilteredOut, 
			Settings policy, boolean isSc
    ) {
        super(cluster, key.namespace, txn, filterExp, failOnFilteredOut, policy);

        if (!isSc) {
            this.key = key;
            this.binNames = binNames;
            this.partition = new Partition(partitions, key, policy.getReplicaOrder(), null, false);
            this.readModeAP = policy.getReadModeAP();
            this.readModeSC = ReadModeSC.SESSION;
            this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
            this.withNoBins = withNoBins;
        }
        else {
            this.key = key;
            this.binNames = binNames;
            this.readModeAP = ReadModeAP.ONE;
            this.readModeSC = policy.getReadModeSC();
            this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
            this.withNoBins = withNoBins;

            Replica replica;
            boolean linearize;

            switch (readModeSC) {
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

            this.partition = new Partition(partitions, key, replica, null, linearize);
        }
    }
}
