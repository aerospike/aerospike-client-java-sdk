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
package com.aerospike.client.fluent.policy;

import java.time.Duration;
import java.util.Objects;

import com.aerospike.client.fluent.policy.Behavior.CommandType;

public class SettableInfoPolicy extends SettablePolicy {
    public static class Builder {
        private final BehaviorBuilder builder;
        private SettableInfoPolicy policy;

        public Builder(BehaviorBuilder builder, CommandType type) {
            this.builder = builder;
            this.policy = new SettableInfoPolicy();
        }

        public SettableInfoPolicy.Builder abandonCallAfter(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            if (intValue < 0) {
                throw new IllegalArgumentException("Duration parameter to abandonCallAfter must reflect a positive time");
            }
            policy.abandonCallAfter = intValue;
            return this;
        }

        public BehaviorBuilder done() {
            builder.setPolicy(CommandType.INFO, policy);
            return builder;
        }

        public SettableInfoPolicy getPolicy() {
            return policy;
        }
    }

    protected void mergeFrom(SettableInfoPolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.abandonCallAfter == null) {
            this.abandonCallAfter = thisPolicy.abandonCallAfter;
        }
    }

    protected InfoPolicy formPolicy(InfoPolicy policy) {
        if (abandonCallAfter != null) {
            policy.timeout = this.abandonCallAfter;
        }
        return policy;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(abandonCallAfter);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
			return true;
		}
        if (getClass() != obj.getClass()) {
			return false;
		}
        SettableInfoPolicy other = (SettableInfoPolicy)obj;
        return Objects.equals(abandonCallAfter, other.abandonCallAfter);
    }
}