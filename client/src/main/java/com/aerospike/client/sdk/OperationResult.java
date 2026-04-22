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
package com.aerospike.client.sdk;

import com.aerospike.client.sdk.Value.GeoJSONValue;
import com.aerospike.client.sdk.Value.HLLValue;

/**
 * Operation result object.
 */
public final class OperationResult {
    private final Object result;

    /**
     * Operation result constructor.
     */
    public OperationResult(Object result) {
        this.result = result;
    }

    /**
     * Get raw object.
     */
    public Object getValue() {
        return result;
    }

    /**
     * Get value as String.
     */
    public String getString() {
        return (String)result;
    }

    /**
     * Get value as byte[].
     */
    public byte[] getBytes() {
        return (byte[])result;
    }

    /**
     * Get value as double.
     */
    public double getDouble() {
        return (result != null)? (double)result : 0.0;
    }

    /**
     * Get value as float.
     */
    public float getFloat() {
        return (float)getDouble();
    }

    /**
     * Get value as long.
     */
    public long getLong() {
        return (result != null)? (long)result : 0;
    }

    /**
     * Get value as int.
     */
    public int getInt() {
        // The server always returns numbers as longs, so get long and cast.
        return (int)getLong();
    }

    /**
     * Get bin value as short.
     */
    public short getShort() {
        // The server always returns numbers as longs, so get long and cast.
        return (short)getLong();
    }

    /**
     * Get bin value as byte.
     */
    public byte getByte() {
        // The server always returns numbers as longs, so get long and cast.
        return (byte)getLong();
    }

    /**
     * Get bin value as boolean.
     */
    public boolean getBoolean() {
        // The server may return boolean as boolean or long (created by older clients).
        if (result instanceof Boolean) {
            return (boolean)result;
        }

        if (result != null) {
            long v = (Long)result;
            return v != 0;
        }
        return false;
    }

    /**
     * Get value as list.
     */
    public AerospikeList<?> getList() {
        return (AerospikeList<?>)result;
    }

    /**
     * Get bin value as map.
     */
    public AerospikeMap<?,?> getMap() {
        return (AerospikeMap<?,?>)result;
    }

    /**
     * Get bin value as GeoJSON Value.
     */
    public GeoJSONValue getGeoJSONValue() {
        return (GeoJSONValue)result;
    }

    /**
     * Get bin value as HLL Value.
     */
    public HLLValue getHLLValue() {
        return (HLLValue)result;
    }
}
