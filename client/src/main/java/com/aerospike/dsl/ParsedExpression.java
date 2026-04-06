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
package com.aerospike.dsl;

import static com.aerospike.dsl.parts.AbstractPart.PartType.EXPRESSION_CONTAINER;
import static com.aerospike.dsl.visitor.VisitorUtils.buildExpr;

import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.ExpressionContainer;

import lombok.Getter;

/**
 * A class to build and store the results of DSL expression parsing: parsed {@code expressionTree}, {@code indexesMap}
 * of given indexes, {@code placeholderValues} to match with placeholders and {@link ParseResult} that holds
 * a potential secondary index {@link Filter} and a potential {@link Exp}.
 */
@Beta
@Getter
public class ParsedExpression {

    private final AbstractPart expressionTree;
    private final Map<String, List<Index>> indexesMap;
    private final String preferredBin;
    private final PlaceholderValues placeholderValues;
    private ParseResult result;

    public ParsedExpression(AbstractPart exprTree, PlaceholderValues placeholderValues,
                            Map<String, List<Index>> indexesMap) {
        this(exprTree, placeholderValues, indexesMap, null);
    }

    public ParsedExpression(AbstractPart exprTree, PlaceholderValues placeholderValues,
                            Map<String, List<Index>> indexesMap, String preferredBin) {
        this.expressionTree = exprTree;
        this.placeholderValues = placeholderValues;
        this.indexesMap = indexesMap;
        this.preferredBin = preferredBin;
    }

    /**
     * @return {@link ParseResult} containing secondary index {@link Filter} and/or filter {@link Exp}.
     * Each can be null in case of invalid or unsupported DSL string
     * @throws DslParseException If there was an error
     */
    public ParseResult getResult() {
        if (result == null) {
            result = getResult(placeholderValues);
        }
        return result;
    }

    /**
     * Traverse expression tree using the given placeholder values
     *
     * @param placeholderValues {@link PlaceholderValues} to match with placeholders by index
     * @return {@link ParseResult} containing secondary index {@link Filter} and/or {@link Exp}.
     * Each can be null in case of invalid or unsupported DSL string
     * @throws DslParseException If there was an error
     */
    public ParseResult getResult(PlaceholderValues placeholderValues) {
        if (expressionTree != null) {
            if (expressionTree.getPartType() == EXPRESSION_CONTAINER) {
                AbstractPart resultPart = buildExpr(
                        (ExpressionContainer) expressionTree, placeholderValues, indexesMap, preferredBin);
                return new ParseResult(resultPart.getFilter(), resultPart.getExp());
            } else {
                return new ParseResult(expressionTree.getFilter(), expressionTree.getExp());
            }
        }
        return new ParseResult(null, null);
    }
}
