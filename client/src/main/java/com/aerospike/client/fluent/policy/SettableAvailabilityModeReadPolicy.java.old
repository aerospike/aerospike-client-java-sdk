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

public class SettableAvailabilityModeReadPolicy extends SettablePolicy {
    ReadModeAP migrationReadConsistency;        // readModeAP

    public static class Builder extends SettablePolicy.BuilderBase<Builder> {
        public Builder(BehaviorBuilder builder, CommandType type) {
            super(builder, type, new SettableAvailabilityModeReadPolicy());
        }

        public SettableAvailabilityModeReadPolicy.Builder migrationReadConsistency(ReadModeAP migrationReadConsistency) {
            getPolicy().migrationReadConsistency = migrationReadConsistency;
            return this;
        }

        public SettableAvailabilityModeReadPolicy getPolicy() {
            return ((SettableAvailabilityModeReadPolicy)policy);
        }
    }

    protected void mergeFrom(SettableAvailabilityModeReadPolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.migrationReadConsistency == null) {
            this.migrationReadConsistency = thisPolicy.migrationReadConsistency;
        }
        super.mergeFrom(thisPolicy);
    }

    protected ReadPolicyAP formPolicy() {
    	return new ReadPolicyAP(this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(migrationReadConsistency);
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
        SettableAvailabilityModeReadPolicy other = (SettableAvailabilityModeReadPolicy) obj;
        return migrationReadConsistency == other.migrationReadConsistency;
    }

    public ReadModeAP getReadModeAP() {
    	return migrationReadConsistency;
    }
}