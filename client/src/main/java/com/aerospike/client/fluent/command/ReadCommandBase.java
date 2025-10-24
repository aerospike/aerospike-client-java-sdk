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
import com.aerospike.client.fluent.Partitions;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.ReadModeSC;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.Settings;

public class ReadCommandBase extends Command {
	final Replica replica;
    final ReadModeAP readModeAP;
    final ReadModeSC readModeSC;
    final int readTouchTtlPercent;
    final boolean linearize;

    public ReadCommandBase(
        Cluster cluster, String namespace, Partitions partitions, Txn txn,
        Expression filterExp, Settings policy
    ) {
        super(cluster, namespace, txn, filterExp, policy);
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();

        if (partitions.scMode) {
            this.readModeAP = ReadModeAP.ONE;
            this.readModeSC = policy.getReadModeSC();

            switch (readModeSC) {
            case SESSION:
                this.replica = Replica.MASTER;
                this.linearize = false;
                break;

            case LINEARIZE:
                Replica replica = policy.getReplicaOrder();

                if (replica == Replica.PREFER_RACK) {
                    replica = Replica.SEQUENCE;
                }
                this.replica = replica;
                this.linearize = true;
                break;

            default:
                this.replica = policy.getReplicaOrder();
                this.linearize = false;
                break;
            }
        }
        else {
            this.replica = policy.getReplicaOrder();
            this.readModeAP = policy.getReadModeAP();
            this.readModeSC = ReadModeSC.SESSION;
            this.linearize = false;
        }
    }
}
