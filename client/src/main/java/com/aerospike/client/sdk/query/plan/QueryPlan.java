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
package com.aerospike.client.sdk.query.plan;

import java.util.Arrays;
import java.util.Objects;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.MsgFieldParser;

/**
 * Immutable result of a server query-plan probe. Holds the packed predicate bytes
 * to replay on execute (field {@code 43}) and, for secondary-index plans, opaque
 * server pins ({@code INDEX_NAME} / {@code INDEX_RANGE}).
 */
public final class QueryPlan {
    private final QuerySelection selection;
    private final String namespace;
    private final String set;
    private final byte[] predicateBytes;
    private final String indexName;
    private final byte[] indexRangeBytes;

    private QueryPlan(
        QuerySelection selection,
        String namespace,
        String set,
        byte[] predicateBytes,
        String indexName,
        byte[] indexRangeBytes
    ) {
        this.selection = selection;
        this.namespace = namespace;
        this.set = set;
        this.predicateBytes = predicateBytes;
        this.indexName = indexName;
        this.indexRangeBytes = indexRangeBytes;
    }

    /**
     * Builds a plan from a probe {@code result_code} and parsed response fields.
     *
     * @param resultCode     probe reply result code
     * @param namespace      namespace sent on the probe
     * @param set            set sent on the probe (may be {@code null})
     * @param predicateBytes packed expression bytes sent in probe field {@code 43}
     * @param fields         parsed response fields
     */
    public static QueryPlan fromProbeResponse(
        int resultCode,
        String namespace,
        String set,
        byte[] predicateBytes,
        MsgFieldParser fields
    ) {
        Objects.requireNonNull(namespace, "namespace must not be null");
        Objects.requireNonNull(predicateBytes, "predicateBytes must not be null");
        Objects.requireNonNull(fields, "fields must not be null");

        byte[] predCopy = Arrays.copyOf(predicateBytes, predicateBytes.length);

        if (resultCode == ResultCode.FILTERED_OUT) {
            return new QueryPlan(QuerySelection.FILTERED_OUT, namespace, set, predCopy, null, null);
        }

        if (resultCode != ResultCode.OK) {
            throw new AerospikeException("Query plan probe failed with result code " + resultCode);
        }

        String indexName = fields.getUtf8Field(FieldType.INDEX_NAME);
        byte[] range = fields.getField(FieldType.INDEX_RANGE);

        if (indexName != null && range != null) {
            return new QueryPlan(
                QuerySelection.SECONDARY_INDEX,
                namespace,
                set,
                predCopy,
                indexName,
                Arrays.copyOf(range, range.length)
            );
        }

        if (indexName == null && range == null) {
            return new QueryPlan(QuerySelection.PRIMARY_INDEX, namespace, set, predCopy, null, null);
        }

        throw new AerospikeException.Parse(
            "Inconsistent query plan response: INDEX_NAME and INDEX_RANGE must both be present or both absent"
        );
    }

    public QuerySelection getSelection() {
        return selection;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSet() {
        return set;
    }

    /**
     * Packed predicate bytes to send in field {@code 43} on execute (same as probe).
     */
    public byte[] getPredicateBytes() {
        return predicateBytes;
    }

    /**
     * Secondary-index registry name from probe field {@code 21}, or {@code null} on PI / filtered-out.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Opaque {@code INDEX_RANGE} bytes from probe field {@code 22}, or {@code null} on PI / filtered-out.
     */
    public byte[] getIndexRangeBytes() {
        return indexRangeBytes;
    }

    public boolean isPrimaryIndex() {
        return selection == QuerySelection.PRIMARY_INDEX;
    }

    public boolean isSecondaryIndex() {
        return selection == QuerySelection.SECONDARY_INDEX;
    }

    public boolean isFilteredOut() {
        return selection == QuerySelection.FILTERED_OUT;
    }
}
