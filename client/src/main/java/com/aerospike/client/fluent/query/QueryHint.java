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
package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.policy.QueryDuration;

/**
 * Type-state configuration for query where-clause hints.
 *
 * <p>A hint allows the caller to influence secondary index selection and query duration
 * for dataset queries that use a {@code where} clause. The three optional properties are:</p>
 * <ul>
 *   <li>{@code forIndex(name)} &ndash; use a specific secondary index by name</li>
 *   <li>{@code forBin(name)} &ndash; prefer the secondary index on a specific bin</li>
 *   <li>{@code queryDuration(duration)} &ndash; override the expected query duration</li>
 * </ul>
 *
 * <p>{@code forIndex} and {@code forBin} are mutually exclusive; each may be called at most once.
 * These constraints are enforced at <em>compile time</em> via a type-state pattern: each method
 * returns a different interface that exposes only the methods still valid at that point.</p>
 *
 * <!-- The codebase typically uses Consumer<T> lambdas for inline configuration.
 *      Here we use Function<Start, ? extends Result> instead, because the valid
 *      permutations are small (7 paths) and compile-time safety that mirrors the
 *      mutual-exclusivity semantics to the user is more important than strict
 *      adherence to the Consumer<T> convention. The user-facing lambda syntax is
 *      virtually identical:  hint -> hint.forIndex("x").queryDuration(SHORT)  -->
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * session.query(dataSet)
 *     .where("$.age > 30")
 *     .withHint(hint -> hint.forIndex("age_idx").queryDuration(QueryDuration.SHORT))
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
    }

    /**
     * Entry-point state &ndash; all three configuration methods are available.
     */
    public interface Start extends Result {
        /** Specify a secondary index by name. */
        AfterTarget forIndex(String indexName);
        /** Prefer the secondary index on the given bin. */
        AfterTarget forBin(String binName);
        /** Override the expected query duration. */
        AfterDuration queryDuration(QueryDuration duration);
    }

    /**
     * State after {@code forIndex()} or {@code forBin()} &ndash; only {@code queryDuration()} remains.
     */
    public interface AfterTarget extends Result {
        /** Override the expected query duration. */
        Result queryDuration(QueryDuration duration);
    }

    /**
     * State after {@code queryDuration()} &ndash; only {@code forIndex()} or {@code forBin()} remains.
     */
    public interface AfterDuration extends Result {
        /** Specify a secondary index by name. */
        Result forIndex(String indexName);
        /** Prefer the secondary index on the given bin. */
        Result forBin(String binName);
    }

    // ---- package-private implementation ------------------------------------------------

    /**
     * Single mutable implementation of all type-state interfaces.
     * Covariant return types allow one class to satisfy every interface contract:
     * e.g. {@code Start.forIndex()} returns {@code AfterTarget} while
     * {@code AfterDuration.forIndex()} returns {@code Result}; since
     * {@code AfterTarget extends Result}, declaring the return type as
     * {@code AfterTarget} satisfies both.
     */
    static final class Impl implements Start, AfterTarget, AfterDuration {
        private String indexName;
        private String binName;
        private QueryDuration queryDuration;

        @Override
        public AfterTarget forIndex(String indexName) {
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

        @Override public String getIndexName()            { return indexName; }
        @Override public String getBinName()              { return binName; }
        @Override public QueryDuration getQueryDuration() { return queryDuration; }
    }

    /**
     * Creates a new hint builder in the {@link Start} state.
     * Used internally by {@link QueryBuilder#withHint}.
     */
    static Start create() {
        return new Impl();
    }
}
