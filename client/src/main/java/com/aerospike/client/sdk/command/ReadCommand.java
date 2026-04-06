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

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.ReadModeAP;
import com.aerospike.client.sdk.policy.ReadModeSC;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.tend.Partition;
import com.aerospike.client.sdk.tend.Partitions;

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
        boolean withNoBins, Expression filterExp, boolean failOnFilteredOut, Settings policy,
        ReadAttr attr
    ) {
        super(cluster, key.namespace, txn, filterExp, attr.replica, policy);
        this.key = key;
        this.partition = new Partition(partitions, key, replica, null, attr.linearize);
        this.readModeAP = attr.readModeAP;
        this.readModeSC = attr.readModeSC;
        this.binNames = binNames;
        this.readTouchTtlPercent = policy.getResetTtlOnReadAtPercent();
        this.withNoBins = withNoBins;
        this.failOnFilteredOut = failOnFilteredOut;
        this.linearize = attr.linearize;
    }
}
