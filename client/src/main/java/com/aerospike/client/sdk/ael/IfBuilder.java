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
package com.aerospike.client.sdk.ael;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for creating IF-THEN-ELSE IF-ELSE expressions.
 * Provides an API for constructing conditional expressions.
 */
public class IfBuilder {
    private final List<BooleanExpression> conditions = new ArrayList<>();
    private final List<AelExpression> results = new ArrayList<>();
    private AelExpression elseResult;

    /**
     * Starts an IF expression with the first condition and result.
     */
    public static IfBuilder if_(BooleanExpression condition, AelExpression result) {
        IfBuilder builder = new IfBuilder();
        builder.conditions.add(condition);
        builder.results.add(result);
        return builder;
    }

    /**
     * Adds an ELSE IF clause.
     */
    public IfBuilder elseIf(BooleanExpression condition, AelExpression result) {
        conditions.add(condition);
        results.add(result);
        return this;
    }

    /**
     * Adds the ELSE clause and builds the final expression.
     */
    public IfExpression else_(AelExpression result) {
        this.elseResult = result;
        return new IfExpression(conditions, results, elseResult);
    }
} 