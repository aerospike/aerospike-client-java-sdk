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

/**
 * Builder for the bins/id/values pattern. Holds the bin names schema and DataSet,
 * requiring at least one {@code .id(...).values(...)} pair before execution.
 *
 * <p>This builder mirrors the SQL {@code INSERT INTO ... (columns) VALUES ...} pattern,
 * co-locating each record's key with its data:</p>
 *
 * <pre>{@code
 * session.insert(customerDataSet)
 *     .bins("name", "age")
 *     .id(900).values("Tim", 312)
 *     .id(901).values("Jane", 28)
 *     .id(902).values("Bob", 54).expireRecordAfter(Duration.ofDays(5))
 *     .execute();
 * }</pre>
 *
 * @see IdValuesRowBuilder
 */
public class IdValuesBuilder {
    private final Session session;
    private final DataSet dataSet;
    private final OpType opType;
    private final String[] binNames;

    IdValuesBuilder(Session session, DataSet dataSet, OpType opType, String binName, String... moreBinNames) {
        this.session = session;
        this.dataSet = dataSet;
        this.opType = opType;
        this.binNames = new String[1 + moreBinNames.length];
        this.binNames[0] = binName;
        System.arraycopy(moreBinNames, 0, this.binNames, 1, moreBinNames.length);
    }

    /**
     * Begin a row for the record with the given integer key.
     *
     * @param id the integer identifier for the record
     * @return an IdValuesRowBuilder for specifying this record's values
     */
    public IdValuesRowBuilder id(int id) {
        return new IdValuesRowBuilder(session, dataSet, opType, binNames).id(id);
    }

    /**
     * Begin a row for the record with the given long key.
     *
     * @param id the long identifier for the record
     * @return an IdValuesRowBuilder for specifying this record's values
     */
    public IdValuesRowBuilder id(long id) {
        return new IdValuesRowBuilder(session, dataSet, opType, binNames).id(id);
    }

    /**
     * Begin a row for the record with the given String key.
     *
     * @param id the String identifier for the record
     * @return an IdValuesRowBuilder for specifying this record's values
     */
    public IdValuesRowBuilder id(String id) {
        return new IdValuesRowBuilder(session, dataSet, opType, binNames).id(id);
    }

    /**
     * Begin a row for the record with the given byte array key.
     *
     * @param id the byte array identifier for the record
     * @return an IdValuesRowBuilder for specifying this record's values
     */
    public IdValuesRowBuilder id(byte[] id) {
        return new IdValuesRowBuilder(session, dataSet, opType, binNames).id(id);
    }
}
