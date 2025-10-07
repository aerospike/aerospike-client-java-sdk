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

import java.util.Objects;

import com.aerospike.client.fluent.policy.Behavior.CommandType;

public class SettableBatchPolicy extends SettablePolicy {
    private Boolean allowInlineMemoryAccess;            // AllowInline
    private Boolean allowInlineSsdAccess;               // AllowInlineSSD
    private Integer maxConcurrentServers;               // maxConcurrentNodes

    public static class Builder extends SettablePolicy.BuilderBase<Builder> {
        public Builder(BehaviorBuilder builder, CommandType type) {
            super(builder, type, new SettableBatchPolicy());
        }
        public SettableBatchPolicy.Builder maxConcurrentServers(int value) {
            checkMinValue(value, 0, "max concurrent servers");
            getPolicy().maxConcurrentServers = value;
            return this;
        }

        public SettableBatchPolicy.Builder allowInlineMemoryAccess(boolean allow) {
            getPolicy().allowInlineMemoryAccess = allow;
            return this;
        }
        public SettableBatchPolicy.Builder allowInlineSsdAccess(boolean allow) {
            getPolicy().allowInlineSsdAccess = allow;
            return this;
        }

        public SettableBatchPolicy getPolicy() {
            return ((SettableBatchPolicy)policy);
        }

    }
    protected void mergeFrom(SettableBatchPolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.allowInlineMemoryAccess == null) {
            this.allowInlineMemoryAccess = thisPolicy.allowInlineMemoryAccess;
        }
        if (this.allowInlineSsdAccess == null) {
            this.allowInlineSsdAccess = thisPolicy.allowInlineSsdAccess;
        }
        if (this.maxConcurrentServers == null) {
            this.maxConcurrentServers = thisPolicy.maxConcurrentServers;
        }
        super.mergeFrom(thisPolicy);
    }

    @SuppressWarnings("deprecation")
	protected BatchPolicy formPolicy(BatchPolicy policy) {
        if (allowInlineMemoryAccess != null) {
            policy.allowInline = allowInlineMemoryAccess;
        }
        if (allowInlineSsdAccess != null) {
            policy.allowInlineSSD = allowInlineSsdAccess;
        }
        if (maxConcurrentServers != null) {
            policy.maxConcurrentThreads = maxConcurrentServers;
        }
        super.formPolicy(policy);
        return policy;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result +
                Objects.hash(allowInlineMemoryAccess, allowInlineSsdAccess, maxConcurrentServers);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
			return true;
		}
        if (!super.equals(obj)) {
			return false;
		}
        if (getClass() != obj.getClass()) {
			return false;
		}
        SettableBatchPolicy other = (SettableBatchPolicy) obj;
        return Objects.equals(allowInlineMemoryAccess, other.allowInlineMemoryAccess)
                && Objects.equals(allowInlineSsdAccess, other.allowInlineSsdAccess)
                && Objects.equals(maxConcurrentServers, other.maxConcurrentServers);
    }


}