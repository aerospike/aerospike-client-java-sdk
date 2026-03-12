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
package com.aerospike.client.fluent.dsl;

import java.util.List;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents an expression with local variable scope.
 * Defines one or more local variables and evaluates an expression in their scope.
 */
public class LocalVariableExpression implements DslExpression {
    private final List<VariableDefinition> variables;
    private final DslExpression resultExpression;

    public LocalVariableExpression(List<VariableDefinition> variables, DslExpression resultExpression) {
        this.variables = variables;
        this.resultExpression = resultExpression;
    }

    public List<VariableDefinition> getVariables() {
        return variables;
    }

    public DslExpression getResultExpression() {
        return resultExpression;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("define(");

        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(variables.get(i));
        }

        sb.append(").thenReturn(").append(resultExpression).append(")");
        return sb.toString();
    }

    @Override
    public String toAerospikeExpr() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exp.let(");

        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(variables.get(i).toAerospikeExpr());
        }

        sb.append(", ").append(resultExpression.toAerospikeExpr()).append(")");
        return sb.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        Exp[] exps = new Exp[variables.size() + 1];
        for (int i = 0; i < variables.size(); i++) {
            exps[i] = Exp.def(variables.get(i).getName(), variables.get(i).getValue().toAerospikeExp());
        }
        exps[variables.size()] = resultExpression.toAerospikeExp();
        return Exp.let(exps);
    }
}