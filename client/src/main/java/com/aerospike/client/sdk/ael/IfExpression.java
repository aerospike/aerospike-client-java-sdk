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

import java.util.List;

import com.aerospike.client.sdk.exp.Exp;

/**
 * Represents an IF-THEN-ELSE IF-ELSE expression.
 * Takes an odd number of arguments: boolean expressions alternating with result expressions.
 * The last expression is the ELSE clause.
 */
public class IfExpression implements DslExpression {
    private final List<BooleanExpression> conditions;
    private final List<DslExpression> results;
    private final DslExpression elseResult;

    public IfExpression(List<BooleanExpression> conditions, List<DslExpression> results, DslExpression elseResult) {
        this.conditions = conditions;
        this.results = results;
        this.elseResult = elseResult;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IF ");

        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                sb.append(" ELSE IF ");
            }
            sb.append(conditions.get(i)).append(" THEN ").append(results.get(i));
        }

        if (elseResult != null) {
            sb.append(" ELSE ").append(elseResult);
        }

        return sb.toString();
    }

    @Override
    public String toAerospikeExpr() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exp.cond(");

        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(conditions.get(i).toAerospikeExpr())
              .append(", ")
              .append(results.get(i).toAerospikeExpr());
        }

        if (elseResult != null) {
            sb.append(", ").append(elseResult.toAerospikeExpr());
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        Exp[] conds = new Exp[conditions.size() * 2 + 1];
        for (int i = 0; i < conditions.size(); i++) {
            conds[i*2] = conditions.get(i).toAerospikeExp();
            conds[i*2+1] = results.get(i).toAerospikeExp();
        }
        conds[conditions.size()*2] = elseResult.toAerospikeExp();
        return Exp.cond(conds);
    }
}