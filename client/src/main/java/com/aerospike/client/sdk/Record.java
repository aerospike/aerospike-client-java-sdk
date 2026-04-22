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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.aerospike.client.sdk.Value.GeoJSONValue;
import com.aerospike.client.sdk.Value.HLLValue;

/**
 * Container object for records.  Records are equivalent to rows.
 */
public final class Record {
    /**
     * Map of requested name/value bins.
     */
    public final Map<String,Object> bins;

    /**
     * Array of result values in the same operation order in the command.
     */
    public final OperationResult[] results;

    /**
     * Record modification count.
     */
    public final int generation;

    /**
     * Date record will expire, in seconds from Jan 01 2010 00:00:00 GMT
     */
    public final int expiration;

    /**
     * Initialize record.
     */
    public Record(
        Map<String,Object> bins,
        OperationResult[] results,
        int generation,
        int expiration
    ) {
        this.bins = bins;
        this.results = results;
        this.generation = generation;
        this.expiration = expiration;
    }

    /**
     * Initialize record from generation and expiration. Bins are not populated.
     */
    public Record(int generation, int expiration) {
        this.bins = new HashMap<>(0);
        this.results = new OperationResult[0];
        this.generation = generation;
        this.expiration = expiration;
    }

    /**
     * Return the command's operation results size.
     */
    public int operationResultCount() {
        return results.length;
    }

    /**
     * Return operation result for the given offset. The results are in the same order as
     * the operations in the command. This is an alternate way to retrieve results as
     * opposed to looking up results by bin name.
     *
     * <pre>{@code
     * RecordStream rs = session.upsert(key)
     *     .bin("name").get()    // op 0
     *     .bin("score").get()   // op 1
     *     .bin("score").add(10) // op 2 (no result)
     *     .bin("score").get()   // op 3
     *     .bin("tags").get()    // op 4
     *     .execute();
     *
     * Record rec = rs.getFirstRecord();
     * String name = rec.operationResult(0).getString();
     * long scoreBefore = rec.operationResult(1).getLong();
     * long scoreAfter = rec.operationResult(3).getLong();
     * List<?> tags = rec.operationResult(4).getList();
     * }</pre>
     */
    public OperationResult operationResult(int offset) {
        return results[offset];
    }

    /**
     * Get bin value given bin name.
     */
    public Object getValue(String name) {
        return bins.get(name);
    }

    /**
     * Get bin value as String.
     */
    public String getString(String name) {
        return (String)getValue(name);
    }

    /**
     * Get bin value as byte[].
     */
    public byte[] getBytes(String name) {
        return (byte[])getValue(name);
    }

    /**
     * Get bin value as double.
     */
    public double getDouble(String name) {
        Object result = getValue(name);
        return (result != null)? (double)result : 0.0;
    }

    /**
     * Get bin value as float.
     */
    public float getFloat(String name) {
        return (float)getDouble(name);
    }

    /**
     * Get bin value as long.
     */
    public long getLong(String name) {
        // The server always returns numbers as longs if bin found.
        // If bin not found, the result will be null.  Convert null to zero.
        Object result = getValue(name);
        return (result != null)? (long)result : 0;
    }

    /**
     * Get bin value as int.
     */
    public int getInt(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (int)getLong(name);
    }

    /**
     * Get bin value as short.
     */
    public short getShort(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (short)getLong(name);
    }

    /**
     * Get bin value as byte.
     */
    public byte getByte(String name) {
        // The server always returns numbers as longs, so get long and cast.
        return (byte)getLong(name);
    }

    /**
     * Get bin value as boolean.
     */
    public boolean getBoolean(String name) {
        // The server may return boolean as boolean or long (created by older clients).
        Object result = getValue(name);

        if (result instanceof Boolean) {
            return (Boolean)result;
        }

        if (result != null) {
            long v = (Long)result;
            return v != 0;
        }
        return false;
    }

    /**
     * Get bin value as list.
     */
    public AerospikeList<?> getList(String name) {
        return (AerospikeList<?>)getValue(name);
    }

    /**
     * Get bin value as map.
     */
    public AerospikeMap<?,?> getMap(String name) {
        return (AerospikeMap<?,?>)getValue(name);
    }

    /**
     * Get the value returned by a UDF execute in a batch.
     * The result may be null.
     */
    public Object getUDFResult() {
        return getValue("SUCCESS");
    }

    /**
     * Get the error string returned by a UDF execute in a batch.
     * Return null if an error did not occur.
     */
    public String getUDFError() {
        return getString("FAILURE");
    }

    /**
     * This method is deprecated. Use {@link #getGeoJSONString(String)} instead.
     *
     * Get bin value as GeoJSON (backward compatibility).
     */
    @Deprecated
    public String getGeoJSON(String name) {
        return getGeoJSONString(name);
    }

    /**
     * Get bin value as GeoJSON String.
     */
    public String getGeoJSONString(String name) {
        Object value = getValue(name);
        return (value != null) ? value.toString() : null;
    }

    /**
     * Get bin value as GeoJSON Value.
     */
    public GeoJSONValue getGeoJSONValue(String name) {
        return (GeoJSONValue)getValue(name);
    }

    /**
     * Get bin value as HLL Value.
     */
    public HLLValue getHLLValue(String name) {
        return (HLLValue)getValue(name);
    }

    /**
     * Convert record expiration (seconds from Jan 01 2010 00:00:00 GMT) to
     * ttl (seconds from now).
     */
    public int getTimeToLive() {
        // This is the server's flag indicating the record never expires.
        if (expiration == 0) {
            // Convert to client-side convention for "never expires".
            return -1;
        }

        // Subtract epoch difference (1970/1/1 GMT to 2010/1/1 GMT) from current time.
        // Handle server's unsigned int ttl with java's usage of long for time.
        int now = (int)((System.currentTimeMillis() - 1262304000000L) / 1000);

        // Record may not have expired on server, but delay or clock differences may
        // cause it to look expired on client. Floor at 1, not 0, to avoid old
        // "never expires" interpretation.
        return ((expiration < 0 && now >= 0) || expiration > now) ? expiration - now : 1;
    }

    /**
     * Return String representation of record.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(500);
        sb.append("(gen:");
        sb.append(generation);
        sb.append("),(exp:");
        sb.append(expiration);
        sb.append("),(bins:");

        if (bins != null) {
            boolean sep = false;

            for (Entry<String,Object> entry : bins.entrySet()) {
                if (sep) {
                    sb.append(',');
                }
                else {
                    sep = true;
                }

                if (sb.length() > 1000) {
                    sb.append("...");
                    break;
                }

                sb.append('(');
                sb.append(entry.getKey());
                sb.append(':');
                sb.append(entry.getValue());
                sb.append(')');
            }
        }
        else {
            sb.append("null");
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(bins, generation, expiration);
    }

    /**
     * Compare records for equality.
     */
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
        Record other = (Record) obj;
        if (expiration != other.expiration) {
            return false;
        }
        if (generation != other.generation) {
            return false;
        }
        if (bins == null) {
            return other.bins == null;
        } else {
            return bins.equals(other.bins);
        }
    }
}
