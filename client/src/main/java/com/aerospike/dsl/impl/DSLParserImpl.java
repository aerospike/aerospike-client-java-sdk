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

        try {
            ParseTree parseTree = getParseTree(pathToCtx);
            return buildCtx(new ExpressionConditionVisitor().visit(parseTree));
        } catch (Exception e) {
            throw new DslParseException("Could not parse the given DSL path input", e);
        }
    }

    private ConditionParser createParser(String input, DSLParserErrorListener errorListener) {
        ConditionLexer lexer = new ConditionLexer(CharStreams.fromString(input));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        ConditionParser parser = new ConditionParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        return parser;
    }

    private ParseTree getParseTree(String input) {
        DSLParserErrorListener errorListener = new DSLParserErrorListener();
        ConditionParser parser = createParser(input, errorListener);
        ParseTree tree = parser.parse();

        String errorMessage = errorListener.getErrorMessage();
        if (errorMessage != null) {
            throw new DslParseException("Could not parse given DSL expression input: " + errorMessage);
        }
        return tree;
    }

    private ParsedExpression getParsedExpression(ParseTree parseTree, PlaceholderValues placeholderValues,
                                                 IndexContext indexContext) {
        final String namespace = Optional.ofNullable(indexContext)
                .map(IndexContext::getNamespace)
                .orElse(null);

        Map<String, List<Index>> indexesMap = buildIndexesMap(
                Optional.ofNullable(indexContext).map(IndexContext::getIndexes).orElse(null), namespace);
        String preferredBin = Optional.ofNullable(indexContext)
                .map(IndexContext::getPreferredBin)
                .orElse(null);

        AbstractPart resultingPart = new ExpressionConditionVisitor().visit(parseTree);

        if (resultingPart == null) {
            throw new DslParseException("Could not parse given DSL expression input");
        }

        return new ParsedExpression(resultingPart, placeholderValues, indexesMap, preferredBin);
    }

    private Map<String, List<Index>> buildIndexesMap(Collection<Index> indexes, String namespace) {
        if (indexes == null || indexes.isEmpty() || namespace == null) return Collections.emptyMap();
        return indexes.stream()
                .filter(idx -> namespace.equals(idx.getNamespace()))
                .collect(Collectors.groupingBy(Index::getBin));
    }
}
