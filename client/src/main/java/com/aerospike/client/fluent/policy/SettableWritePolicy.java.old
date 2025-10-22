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

public class SettableWritePolicy extends SettablePolicy {
	Boolean durableDelete;      // durableDelete

    public static class Builder extends SettablePolicy.BuilderBase<Builder> {
        public Builder(BehaviorBuilder builder, CommandType type) {
            super(builder, type, new SettableWritePolicy());
        }

        public SettableWritePolicy.Builder useDurableDelete(boolean useDurableDelete) {
            getPolicy().durableDelete = useDurableDelete;
            return this;
        }

        public SettableWritePolicy getPolicy() {
            return ((SettableWritePolicy)policy);
        }
    }

    protected void mergeFrom(SettableWritePolicy thisPolicy) {
        if (thisPolicy == null) {
            return;
        }
        if (this.durableDelete == null) {
            this.durableDelete = thisPolicy.durableDelete;
        }
        super.mergeFrom(thisPolicy);
    }

    protected WritePolicy formPolicy() {
    	return new WritePolicy(this);
    }

    public boolean getDurableDelete() {
    	return (durableDelete != null) ? durableDelete : false;
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(durableDelete);
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
        SettableWritePolicy other = (SettableWritePolicy) obj;
        return Objects.equals(durableDelete, other.durableDelete);
    }
}