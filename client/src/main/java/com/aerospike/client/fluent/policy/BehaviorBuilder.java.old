/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.fluent.policy;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.policy.Behavior.CommandType;

public class BehaviorBuilder {
    private Map<CommandType, SettablePolicy> policies = new HashMap<>();

    protected BehaviorBuilder setPolicy(CommandType type, SettablePolicy policy) {
        this.policies.put(type, policy);
        return this;
    }

    protected Map<CommandType, SettablePolicy> getPolicies() {
        return this.policies;
    }

    public SettablePolicy.Builder forAllOperations() {
        return new SettablePolicy.Builder(this, CommandType.ALL, new SettablePolicy());
    }
    public SettableConsistencyModeReadPolicy.Builder onConsistencyModeReads() {
        return new SettableConsistencyModeReadPolicy.Builder(this, CommandType.READ_SC);
    }
    public SettableAvailabilityModeReadPolicy.Builder onAvailablityModeReads() {
        return new SettableAvailabilityModeReadPolicy.Builder(this, CommandType.READ_AP);
    }
    public SettableWritePolicy.Builder onRetryableWrites() {
        return new SettableWritePolicy.Builder(this, CommandType.WRITE_RETRYABLE);
    }
    public SettableWritePolicy.Builder onNonRetryableWrites() {
        return new SettableWritePolicy.Builder(this, CommandType.WRITE_NON_RETRYABLE);
    }
    public SettableBatchPolicy.Builder onBatchReads() {
        return new SettableBatchPolicy.Builder(this, CommandType.BATCH_READ);
    }
    public SettableBatchPolicy.Builder onBatchWrites() {
        return new SettableBatchPolicy.Builder(this, CommandType.BATCH_WRITE);
    }
    public SettableQueryPolicy.Builder onQuery() {
        return new SettableQueryPolicy.Builder(this, CommandType.QUERY);
    }
    public SettableInfoPolicy.Builder onInfo() {
        return new SettableInfoPolicy.Builder(this, CommandType.INFO);
    }

}