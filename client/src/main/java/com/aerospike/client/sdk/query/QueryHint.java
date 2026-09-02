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
package com.aerospike.client.sdk.query;

import com.aerospike.client.sdk.policy.QueryDuration;

/**
 * Type-state configuration for query where-clause hints.
 *
 * <p>A hint allows the caller to influence secondary index selection and query duration
 * for dataset queries that use a {@code where} clause. Optional properties:</p>
 * <ul>
 *   <li>{@code forIndex(name)} &ndash; soft index name hint (field {@code 21} on explain)</li>
 *   <li>{@code forBin(name)} &ndash; prefer the secondary index on a specific bin (legacy path)</li>
 *   <li>{@code requireIndex()} &ndash; reject primary-index fallback on explain ({@code REQUIRE_INDEX})</li>
 *   <li>{@code hardHint()} &ndash; after {@code forIndex}, require that index only ({@code HARD_HINT})</li>
 *   <li>{@code queryDuration(duration)} &ndash; override the expected query duration</li>
 * </ul>
 *
 * <p>{@code forIndex} and {@code forBin} are mutually exclusive; each may be called at most once.
 * These constraints are enforced at <em>compile time</em> via a type-state pattern: each method
 * returns a different interface that exposes only the methods still valid at that point.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * session.query(dataSet)
 *     .where("$.age > 30")
 *     .withHint(hint -> hint.forIndex("age_idx").hardHint().queryDuration(QueryDuration.SHORT))
 *     .execute();
 * }</pre>
 */
public final class QueryHint {

    private QueryHint() {}

    /**
     * Read-only view of the configured hint values.
     * Every type-state interface extends this, so any intermediate or terminal state
     * can be returned from the {@code withHint} lambda.
     */
    public interface Result {
        /** @return the explicit index name, or {@code null} if not set */
        String getIndexName();
        /** @return the preferred bin name, or {@code null} if not set */
        String getBinName();
        /** @return the query duration override, or {@code null} if not set */
        QueryDuration getQueryDuration();
        /** @return whether scan should be allowed on query with a where clause */
        Boolean getAllowScansWithWhere();
        /** @return whether the index name hint is strict ({@code HARD_HINT}) */
        boolean isHardHint();
    }

    /**
     * Entry-point state &ndash; all configuration methods are available.
     */
    public interface Start extends Result {
        /** Specify a secondary index by name (soft hint). */
        AfterIndex forIndex(String indexName);
        /** Prefer the secondary index on the given bin. */
        AfterTarget forBin(String binName);
        /** Override the expected query duration. */
        AfterDuration queryDuration(QueryDuration duration);
        /** Allow primary index scans with where clause. Secondary index queries are not affected. */
        Start allowScansWithWhere();
        /** Disallow primary index scans with where clause. Secondary index queries are not affected. */
        Start disallowScansWithWhere();
    }

    /**
     * State after {@code forIndex()} &ndash; strict hint and duration remain.
     */
    public interface AfterIndex extends AfterTarget {
        /** Require the hinted index name on explain (no soft fallback). */
        Result hardHint();
        /** Allow primary index scans with where clause. Secondary index queries are not affected. */
        AfterIndex allowScansWithWhere();
        /** Disallow primary index scans with where clause. Secondary index queries are not affected. */
        AfterIndex disallowScansWithWhere();
    }

    /**
     * State after {@code forBin()} &ndash; only {@code queryDuration()} remains.
     */
    public interface AfterTarget extends Result {
        /** Override the expected query duration. */
        Result queryDuration(QueryDuration duration);
    }

    /**
     * State after {@code queryDuration()} &ndash; only {@code forIndex()} or {@code forBin()} remains.
     */
    public interface AfterDuration extends Result {
        /** Specify a secondary index by name (soft hint). */
        AfterIndex forIndex(String indexName);
        /** Prefer the secondary index on the given bin. */
        AfterTarget forBin(String binName);
        /** Allow primary index scans with where clause. Secondary index queries are not affected. */
        AfterDuration allowScansWithWhere();
        /** Disallow primary index scans with where clause. Secondary index queries are not affected. */
        AfterDuration disallowScansWithWhere();
    }

  // ---- package-private implementation ------------------------------------------------

    /**
     * Single mutable implementation of all type-state interfaces.
     */
    static final class Impl implements Start, AfterIndex, AfterDuration {
        private String indexName;
        private String binName;
        private Boolean allowScansWithWhere;
        private QueryDuration queryDuration;
        private boolean hardHint;

        @Override
        public AfterIndex forIndex(String indexName) {
            this.indexName = indexName;
            return this;
        }

        @Override
        public AfterTarget forBin(String binName) {
            this.binName = binName;
            return this;
        }

        @Override
        public AfterDuration queryDuration(QueryDuration duration) {
            this.queryDuration = duration;
            return this;
        }

        @Override
        public Impl allowScansWithWhere() {
            this.allowScansWithWhere = true;
            return this;
        }

        @Override
        public Impl disallowScansWithWhere() {
            this.allowScansWithWhere = false;
            return this;
        }

        @Override
        public Result hardHint() {
            if (indexName == null || indexName.isBlank()) {
                throw new IllegalArgumentException(
                    "hardHint requires forIndex with a non-blank index name");
            }
            this.hardHint = true;
            return this;
        }

        @Override public String getIndexName()            { return indexName; }
        @Override public String getBinName()              { return binName; }
        @Override public QueryDuration getQueryDuration() { return queryDuration; }
        @Override public boolean isHardHint()             { return hardHint; }
        @Override public Boolean getAllowScansWithWhere() { return allowScansWithWhere; }
    }

    /**
     * Creates a new hint builder in the {@link Start} state.
     * Used internally by {@link QueryBuilder#withHint}.
     */
    static Start create() {
        return new Impl();
    }
}
