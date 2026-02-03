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

import java.util.function.Consumer;

import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.ExpReadFlags;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.query.PreparedDsl;

/**
 * Builder for bin-level read operations in query contexts.
 * 
 * <p>This builder is used by {@link ChainableQueryBuilder} for query operations
 * and only supports read operations (no write operations like setTo, insertFrom, etc.)</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * session.query(key)
 *     .bin("name").get()
 *     .bin("ageIn20Years").selectFrom("$.age + 20")
 *     .execute();
 * }</pre>
 */
public class QueryBinBuilder {
    private final ChainableQueryBuilder queryBuilder;
    private final String binName;

    QueryBinBuilder(ChainableQueryBuilder queryBuilder, String binName) {
        this.queryBuilder = queryBuilder;
        this.binName = binName;
    }

    /**
     * Create a read bin operation.
     *
     * @return the query builder for method chaining
     */
    public ChainableQueryBuilder get() {
        queryBuilder.addOperation(Operation.get(binName));
        return queryBuilder;
    }

    // ----------------------------------------
    // selectFrom - Read expression operations
    // Supports all 5 DSL input types
    // ----------------------------------------

    /** Create a read expression from a DSL string. */
    public ChainableQueryBuilder selectFrom(String dsl) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a DSL string with options. */
    public ChainableQueryBuilder selectFrom(String dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from a BooleanExpression. */
    public ChainableQueryBuilder selectFrom(BooleanExpression dsl) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a BooleanExpression with options. */
    public ChainableQueryBuilder selectFrom(BooleanExpression dsl, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from a PreparedDsl. */
    public ChainableQueryBuilder selectFrom(PreparedDsl dsl, Object... params) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, params, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from a PreparedDsl with options. */
    public ChainableQueryBuilder selectFrom(PreparedDsl dsl, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, dsl, params, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from an Exp. */
    public ChainableQueryBuilder selectFrom(Exp exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from an Exp with options. */
    public ChainableQueryBuilder selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
    }

    /** Create a read expression from an Expression. */
    public ChainableQueryBuilder selectFrom(Expression exp) {
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
        return queryBuilder;
    }

    /** Create a read expression from an Expression with options. */
    public ChainableQueryBuilder selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        queryBuilder.addOperation(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
        return queryBuilder;
    }
}
