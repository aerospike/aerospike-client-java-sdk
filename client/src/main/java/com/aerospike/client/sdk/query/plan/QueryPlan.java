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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.FieldType;
import com.aerospike.client.sdk.command.MsgFieldParser;
import com.aerospike.client.sdk.query.IndexCollectionType;
import com.aerospike.client.sdk.util.ContainerString;

/**
 * Result of a server query explain (phase 1). Holds field {@code 44} explain bytes,
 * optional SI pins ({@code INDEX_NAME} / {@code INDEX_TYPE} / {@code INDEX_RANGE}),
 * and derived execute WHERE payload.
 */
public final class QueryPlan {
    private final QuerySelection selection;
    private final String namespace;
    private final String set;
    private final byte[] explainWhereBytes;
    private final String indexName;
    private final byte[] indexRangeBytes;
    private final IndexCollectionType indexType;

    private QueryPlan(
        QuerySelection selection,
        String namespace,
        String set,
        byte[] explainWhereBytes,
        String indexName,
        byte[] indexRangeBytes,
        IndexCollectionType indexType
    ) {
        this.selection = selection;
        this.namespace = namespace;
        this.set = set;
        this.explainWhereBytes = explainWhereBytes;
        this.indexName = indexName;
        this.indexRangeBytes = indexRangeBytes;
        this.indexType = indexType;
    }

    /**
     * Builds a plan from an explain {@code result_code} and parsed response fields.
     *
     * @param resultCode        explain reply result code
     * @param namespace         namespace sent on explain
     * @param set               set sent on explain (may be {@code null})
     * @param explainWhereBytes field {@code 44} body sent on explain ({@code EXPLAIN} flag set)
     * @param fields            parsed response fields
     */
    public static QueryPlan fromExplainResponse(
        int resultCode,
        String namespace,
        String set,
        byte[] explainWhereBytes,
        MsgFieldParser fields
    ) {
        if (resultCode == ResultCode.FILTERED_OUT) {
            return new QueryPlan(
                QuerySelection.FILTERED_OUT, namespace, set, explainWhereBytes,
                null, null, IndexCollectionType.DEFAULT);
        }

        if (resultCode != ResultCode.OK) {
            throw new AerospikeException("Query explain failed with result code " + resultCode);
        }

        String indexName = fields.getUtf8Field(FieldType.INDEX_NAME);
        byte[] range = fields.getField(FieldType.INDEX_RANGE);
        IndexCollectionType indexType = fields.getIndexCollectionType();

        boolean hasIndexName = indexName != null;
        boolean hasIndexRange = range != null;

        if (hasIndexName != hasIndexRange) {
            throw new AerospikeException.Parse(
                "Inconsistent query plan response: INDEX_NAME and INDEX_RANGE must both be present or both absent"
            );
        }

        if (hasIndexName) {
            if (indexName.isEmpty() || range.length == 0) {
                throw new AerospikeException.Parse(
                    "Inconsistent query plan response: INDEX_NAME and INDEX_RANGE must be non-empty on SI explain"
                );
            }
            validateSiExplainRange(range);
            return new QueryPlan(
                QuerySelection.SECONDARY_INDEX,
                namespace,
                set,
                explainWhereBytes,
                indexName,
                range,
                indexType
            );
        }

        return new QueryPlan(
            QuerySelection.PRIMARY_INDEX, namespace, set, explainWhereBytes,
            null, null, IndexCollectionType.DEFAULT);
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
     * AEL source text from the explain field {@code 44} payload.
     */
    public String getAel() {
        return QueryWhereWire.ael(explainWhereBytes);
    }

    /**
     * Field {@code 44} body sent on explain ({@link QueryWhereWire#FLAG_EXPLAIN} set).
     */
    public byte[] getExplainWhereBytes() {
        return explainWhereBytes;
    }

    /**
     * Field {@code 44} body for execute ({@link QueryWhereWire#FLAG_EXPLAIN} cleared).
     */
    public byte[] getExecuteWhereBytes() {
        return QueryWhereWire.clearExplain(explainWhereBytes);
    }

    /**
     * Secondary-index registry name from explain field {@code 21}, or {@code null} on PI / filtered-out.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Opaque {@code INDEX_RANGE} bytes from explain field {@code 22} (probe shape), or {@code null} on PI.
     */
    public byte[] getIndexRangeBytes() {
        return indexRangeBytes;
    }

    /**
     * Index collection type from explain field {@code 26}, or {@link IndexCollectionType#DEFAULT} when absent.
     */
    public IndexCollectionType getIndexType() {
        return indexType;
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

    private static void validateSiExplainRange(byte[] range) {
        if (range.length < 2 || (range[0] & 0xFF) != 1) {
            throw new AerospikeException.Parse(
                "Inconsistent query plan response: INDEX_RANGE must start with n_ranges=1");
        }

        int binNameLen = range[1] & 0xFF;
        if (binNameLen > 0 && range.length < 2 + binNameLen + 1) {
            throw new AerospikeException.Parse(
                "Inconsistent query plan response: INDEX_RANGE truncated");
        }
    }

    @Override
    public String toString() {
        return "QueryPlan{selection=" + selection +
            ", namespace=" + namespace +
            ", set=" + set +
            ", ael=" + ContainerString.format(getAel()) +
            ", indexName=" + indexName +
            ", indexType=" + indexType +
            ", indexRange=" + ContainerString.format(IndexRangeWire.describeProbeRange(indexRangeBytes)) +
            '}';
    }
}
