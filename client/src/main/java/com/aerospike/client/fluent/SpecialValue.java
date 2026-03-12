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
package com.aerospike.client.fluent;

/**
 * Represents special boundary values for Aerospike CDT operations.
 * These correspond to the special Value constants in the Aerospike client.
 * 
 * <ul>
 * <li>NULL - Represents null value boundary</li>
 * <li>INFINITY - Represents positive infinity, useful for "to end" range operations</li>
 * <li>WILDCARD - Represents wildcard matching</li>
 * </ul>
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * // Get all scores from 80 to infinity
 * session.update(key)
 *     .bin("scores")
 *     .onMapValueRange(80L, SpecialValue.INFINITY)
 *     .getValues()
 *     .execute();
 * 
 * // Range from null to specific value
 * session.update(key)
 *     .bin("data")
 *     .onMapKeyRange(SpecialValue.NULL, "endKey")
 *     .getKeys()
 *     .execute();
 * }</pre>
 */
public enum SpecialValue {
    /**
     * Represents a null value boundary
     */
    NULL(Value.NULL),
    
    /**
     * Represents positive infinity - useful for unbounded range end
     */
    INFINITY(Value.INFINITY),
    
    /**
     * Represents wildcard matching
     */
    WILDCARD(Value.WILDCARD);
    
    private final Value aerospikeValue;
    
    SpecialValue(Value aerospikeValue) {
        this.aerospikeValue = aerospikeValue;
    }
    
    /**
     * Gets the corresponding Aerospike Value constant
     * @return the Aerospike Value
     */
    public Value toAerospikeValue() {
        return aerospikeValue;
    }
}

