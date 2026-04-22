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

import com.aerospike.client.sdk.policy.ReadModeAP;
import com.aerospike.client.sdk.policy.ReadModeSC;
import com.aerospike.client.sdk.policy.Replica;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

public class ReadAttr {
    public final ReadModeAP readModeAP;
    public final ReadModeSC readModeSC;
    public final Replica replica;
    public final boolean linearize;

    public ReadAttr(Partitions partitions, ResolvedSettings settings) {
        if (partitions.scMode) {
            readModeAP = ReadModeAP.ONE;
            readModeSC = settings.getReadModeSC();

            switch (readModeSC) {
            case SESSION:
                this.replica = Replica.MASTER;
                this.linearize = false;
                break;

            case LINEARIZE:
                if (settings.getReplicaOrder() == Replica.PREFER_RACK) {
                    this.replica = Replica.SEQUENCE;
                }
                else {
                    this.replica = settings.getReplicaOrder();
                }
                this.linearize = true;
                break;

            default:
                this.replica = settings.getReplicaOrder();
                this.linearize = false;
                break;
            }

        }
        else {
            this.readModeAP = settings.getReadModeAP();
            this.readModeSC = ReadModeSC.SESSION;
            this.replica = settings.getReplicaOrder();
            this.linearize = false;
        }
    }
}
