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
package com.aerospike.dsl.impl;

import static com.aerospike.dsl.visitor.VisitorUtils.buildCtx;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.dsl.ConditionLexer;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.annotation.Beta;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.visitor.ExpressionConditionVisitor;

public class DSLParserImpl implements DSLParser {

    @Override
    @Beta
    public ParsedExpression parseExpression(ExpressionContext expressionContext) {
        ParseTree parseTree = getParseTree(expressionContext.getExpression());
        return getParsedExpression(parseTree, expressionContext.getValues(), null);
    }

    @Override
    @Beta
    public ParsedExpression parseExpression(ExpressionContext expressionContext, IndexContext indexContext) {
        ParseTree parseTree = getParseTree(expressionContext.getExpression());
        return getParsedExpression(parseTree, expressionContext.getValues(), indexContext);
    }

    @Override
    @Beta
    public CTX[] parseCTX(String pathToCtx) {
        if (pathToCtx == null || pathToCtx.isBlank()) {
            throw new DslParseException("Path must not be null or empty");
        }

        ParseTree parseTree = getParseTree(pathToCtx);
        try {
            return buildCtx(new ExpressionConditionVisitor().visit(parseTree));
        } catch (Exception e) {
            throw new DslParseException("Could not parse the given DSL path input", e);
        }
    }

    private ParseTree getParseTree(String input) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        ConditionParser parser = new ConditionParser(new CommonTokenStream(lexer));
        return parser.parse();
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, PlaceholderValues placeholderValues,
                                                 IndexContext indexContext) {
        final String namespace = Optional.ofNullable(indexContext)
                .map(IndexContext::namespace)
                .orElse(null);
        final Collection<Index> indexes = Optional.ofNullable(indexContext)
                .map(IndexContext::indexes)
                .orElse(Collections.emptyList());

        Map<String, List<Index>> indexesMap = indexes.stream()
                // Filtering the indexes with the given namespace
                .filter(idx -> idx.getNamespace() != null && idx.getNamespace().equals(namespace))
                // Group the indexes by bin name
                .collect(Collectors.groupingBy(Index::getBin));

        AbstractPart resultingPart = new ExpressionConditionVisitor().visit(parseTree);

        // When we can't identify a specific case of syntax error, we throw a generic DSL syntax error
        if (resultingPart == null) {
            throw new DslParseException("Could not parse given DSL expression input");
        }

        // Return the parsed tree along with indexes Map
        return new ParsedExpression(resultingPart, placeholderValues, indexesMap);
    }
}
