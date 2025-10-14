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

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import com.aerospike.client.fluent.policy.Behavior.CommandType;

public class SettablePolicy {
	Integer maximumNumberOfCallAttempts;           // maxRetries + 1
    Integer resetTtlOnReadAtPercent;               // readTouchTtlPercent
    Integer waitForCallToComplete;                 // socketTimeout
    Integer waitForSocketResponseAfterCallFails;   // timeoutDelay
    // Info policies really aren't settable policies as it only has an abandonCallAfter setting.
    // However, it makes the code cleaner to treat them as if they are, but that means we need to
    // set this field from the sub-class
    Integer abandonCallAfter;                      // TotalTimeout
    Boolean useCompression;                        // compress
    Integer waitForConnectionToComplete;           // connectTimeout
    Integer delayBetweenRetries;                   // sleepBetweenRetries
    List<NodeCategory> replicaOrder;               // replica
    Boolean sendKey;                               // sendKey

    // We have to use self-referential generics for a fluent pattern
    // This generic template builder is not normally instanitated directly
    static class BuilderBase<T extends BuilderBase<T>> {
        private final BehaviorBuilder builder;
        private final CommandType type;
        protected final SettablePolicy policy;

        public BuilderBase(BehaviorBuilder builder, CommandType type, SettablePolicy thePolicy) {
            this.policy = thePolicy;
            this.builder = builder;
            this.type = type;
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T)this;
        }

        public T useCompression(boolean compress) {
            policy.useCompression = compress;
            return self();
        }

        public T sendKey(boolean sendKey) {
            policy.sendKey = sendKey;
            return self();
        }

        public T waitForConnectionToComplete(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            checkMinValue(intValue, 0, "waitForConnectionToComplete");
            policy.waitForConnectionToComplete = intValue;
            return self();
        }

        public T maximumNumberOfCallAttempts(int maxCalls) {
            checkMinValue(maxCalls, 1, "maximumNumberOfCallAttempts");
            policy.maximumNumberOfCallAttempts = maxCalls;
            return self();
        }

        public T resetTtlOnReadAtPercent(int percent) {
            checkMinValue(percent, 0, "resetTtlOnReadAtPercent");
            checkMaxValue(percent, 100, "resetTtlOnReadAtPercent");
            policy.resetTtlOnReadAtPercent = percent;
            return self();
        }

        public T waitForCallToComplete(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            checkMinValue(intValue, 0, "waitForCallToComplete");
            policy.waitForCallToComplete = intValue;
            return self();
        }

        public T waitForSocketResponseAfterCallFails(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            checkMinValue(intValue, 0, "waitForSocketResponseAfterCallFails");
            policy.waitForSocketResponseAfterCallFails = intValue;
            return self();
        }

        public T abandonCallAfter(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            checkMinValue(intValue, 0, "abandonCallAfter");
            policy.abandonCallAfter = intValue;
            return self();
        }

        public T delayBetweenRetries(Duration duration) {
            long value = duration.toMillis();
            int intValue = value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)value;
            checkMinValue(intValue, 0, "delayBetweenRetries");
            policy.delayBetweenRetries = intValue;
            return self();
        }

        public T replicaOrder(List<NodeCategory> replicaOrder) {
            if (replicaOrder == null || replicaOrder.isEmpty()) {
                throw new IllegalArgumentException("Node categories must be specified.");
            }
            policy.replicaOrder = replicaOrder;
            return self();
        }


        protected void checkMinValue(int value, int minValue, String desc) {
            if (value < minValue) {
                throw new IllegalArgumentException(
                        String.format("Value of %s must be %,d or greater, not %,d", desc, minValue, value));
            }
        }
        protected void checkMaxValue(int value, int minValue, String desc) {
            if (value > minValue) {
                throw new IllegalArgumentException(
                        String.format("Value of %s must be %,d or greater, not %,d", desc, minValue, value));
            }
        }
        public BehaviorBuilder done() {
            builder.setPolicy(type, policy);
            return builder;
        }
    }

    // Concrete builder for the settablePolicy itself
    public static class Builder extends BuilderBase<Builder> {
        public Builder(BehaviorBuilder builder, CommandType type, SettablePolicy thePolicy) {
            super(builder, type, thePolicy);
        }
    }

    protected void mergeFrom(SettablePolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.maximumNumberOfCallAttempts == null) {
            this.maximumNumberOfCallAttempts = thisPolicy.maximumNumberOfCallAttempts;
        }
        if (this.resetTtlOnReadAtPercent == null) {
            this.resetTtlOnReadAtPercent = thisPolicy.resetTtlOnReadAtPercent;
        }
        if (this.waitForCallToComplete == null) {
            this.waitForCallToComplete = thisPolicy.waitForCallToComplete;
        }
        if (this.waitForSocketResponseAfterCallFails == null) {
            this.waitForSocketResponseAfterCallFails = thisPolicy.waitForSocketResponseAfterCallFails;
        }
        if (this.abandonCallAfter == null) {
            this.abandonCallAfter = thisPolicy.abandonCallAfter;
        }
        if (this.useCompression == null) {
            this.useCompression = thisPolicy.useCompression;
        }
        if (this.waitForConnectionToComplete == null) {
            this.waitForConnectionToComplete = thisPolicy.waitForConnectionToComplete;
        }
        if (this.delayBetweenRetries == null) {
            this.delayBetweenRetries = thisPolicy.delayBetweenRetries;
        }
        if (this.replicaOrder == null) {
            this.replicaOrder = thisPolicy.replicaOrder;
        }
        if (this.sendKey == null) {
            this.sendKey = thisPolicy.sendKey;
        }
    }

    protected Policy formPolicy() {
    	return new Policy(this);
    }

	public Replica getReplica() {
		// TODO Convert replicaOrder to replica.
		return Replica.SEQUENCE;
	}

	public int getConnectTimeout() {
		return (waitForConnectionToComplete != null)? waitForConnectionToComplete : 0;
	}

	public int getSocketTimeout() {
		return (waitForCallToComplete != null)? waitForCallToComplete : 30000;
	}

	public int getTotalTimeout() {
		return (abandonCallAfter != null)? abandonCallAfter : 1000;
	}

	public int getTimeoutDelay() {
		return (waitForSocketResponseAfterCallFails != null)? waitForSocketResponseAfterCallFails : 0;
	}

	public int getMaxRetries() {
		return (maximumNumberOfCallAttempts != null)? maximumNumberOfCallAttempts - 1: 2;
	}

	public int getSleepBetweenRetries() {
		return (delayBetweenRetries != null)? delayBetweenRetries : 0;
	}

	public int getReadTouchTtlPercent() {
		return (resetTtlOnReadAtPercent != null)? resetTtlOnReadAtPercent : 0;
	}

	public boolean getSendKey() {
		return (sendKey != null)? sendKey : false;
	}

	public boolean getCompress() {
		return (useCompression != null)? useCompression : false;
	}

    @Override
    public int hashCode() {
        return Objects.hash(abandonCallAfter, delayBetweenRetries, maximumNumberOfCallAttempts, replicaOrder,
                resetTtlOnReadAtPercent, sendKey, useCompression, waitForCallToComplete, waitForConnectionToComplete,
                waitForSocketResponseAfterCallFails);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
			return true;
		}
        if (obj == null) {
			return false;
		}
        if (getClass() != obj.getClass()) {
			return false;
		}
        SettablePolicy other = (SettablePolicy) obj;
        return Objects.equals(abandonCallAfter, other.abandonCallAfter)
                && Objects.equals(delayBetweenRetries, other.delayBetweenRetries)
                && Objects.equals(maximumNumberOfCallAttempts, other.maximumNumberOfCallAttempts)
                && Objects.equals(replicaOrder, other.replicaOrder)
                && Objects.equals(resetTtlOnReadAtPercent, other.resetTtlOnReadAtPercent)
                && Objects.equals(sendKey, other.sendKey) && Objects.equals(useCompression, other.useCompression)
                && Objects.equals(waitForCallToComplete, other.waitForCallToComplete)
                && Objects.equals(waitForConnectionToComplete, other.waitForConnectionToComplete)
                && Objects.equals(waitForSocketResponseAfterCallFails, other.waitForSocketResponseAfterCallFails);
    }
}