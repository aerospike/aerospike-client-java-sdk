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

public class SettableQueryPolicy extends SettablePolicy {
    Integer recordQueueSize;
    Integer maxConcurrentServers;

    public static class Builder extends SettablePolicy.BuilderBase<Builder> {
        public Builder(BehaviorBuilder builder, CommandType type) {
            super(builder, type, new SettableQueryPolicy());
        }

        public SettableQueryPolicy.Builder recordQueueSize(int queueSize) {
            getPolicy().recordQueueSize = queueSize;
            return this;
        }
        public SettableQueryPolicy.Builder maxConcurrentServers(int value) {
            checkMinValue(value, 0, "max concurrent servers");
            getPolicy().maxConcurrentServers = value;
            return this;
        }

        public SettableQueryPolicy getPolicy() {
            return ((SettableQueryPolicy)policy);
        }
    }
    protected void mergeFrom(SettableQueryPolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.recordQueueSize == null) {
            this.recordQueueSize = thisPolicy.recordQueueSize;
        }
        if (this.maxConcurrentServers == null) {
            maxConcurrentServers = thisPolicy.maxConcurrentServers;
        }
        super.mergeFrom(thisPolicy);
    }

    protected QueryPolicy formPolicy() {
    	return new QueryPolicy(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(maxConcurrentServers, recordQueueSize);
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
        SettableQueryPolicy other = (SettableQueryPolicy) obj;
        return Objects.equals(maxConcurrentServers, other.maxConcurrentServers)
                && Objects.equals(recordQueueSize, other.recordQueueSize);
    }


}