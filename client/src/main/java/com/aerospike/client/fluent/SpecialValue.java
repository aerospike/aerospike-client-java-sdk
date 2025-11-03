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

