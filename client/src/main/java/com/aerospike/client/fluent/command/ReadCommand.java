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
	final boolean failOnFilteredOut;
    final boolean linearize;

    public ReadCommand(
        Cluster cluster, Partitions partitions, Txn txn, Key key, String[] binNames,
        Replica replica, ReadModeAP readModeAP, ReadModeSC readModeSC, boolean linearize,
        boolean withNoBins, Expression filterExp, boolean failOnFilteredOut, Settings policy
    ) {
        super(cluster, key.namespace, txn, filterExp, replica, policy);
        this.key = key;
        this.partition = new Partition(partitions, key, replica, null, linearize);
        this.readModeAP = readModeAP;
        this.readModeSC = readModeSC;
        this.binNames = binNames;
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
        this.withNoBins = withNoBins;
        this.failOnFilteredOut = failOnFilteredOut;
        this.linearize = linearize;
    }
}
