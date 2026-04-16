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
package com.aerospike.ael.impl;

import static com.aerospike.ael.visitor.VisitorUtils.buildCtx;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.PlaceholderValues;
import com.aerospike.ael.annotation.Beta;
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.visitor.ExpressionConditionVisitor;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.ael.ConditionLexer;
import com.aerospike.ael.ConditionParser;

public class AelParserImpl implements AelParser {

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
            throw new AelParseException("Path must not be null or empty");
        }

        try {
            ParseTree parseTree = getParseTree(pathToCtx);
            return buildCtx(new ExpressionConditionVisitor().visit(parseTree));
        } catch (Exception e) {
            throw new AelParseException("Could not parse the given AEL path input", e);
        }
    }

    private ConditionParser createParser(String input, AelParserErrorListener errorListener) {
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
        AelParserErrorListener errorListener = new AelParserErrorListener();
        ConditionParser parser = createParser(input, errorListener);
        ParseTree tree = parser.parse();

        String errorMessage = errorListener.getErrorMessage();
        if (errorMessage != null) {
            throw new AelParseException("Could not parse given AEL expression input: " + errorMessage);
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
            throw new AelParseException("Could not parse given AEL expression input");
        }

        return new ParsedExpression(resultingPart, placeholderValues, indexesMap, preferredBin);
    }

    private Map<String, List<Index>> buildIndexesMap(Collection<Index> indexes, String namespace) {
        if (indexes == null || indexes.isEmpty() || namespace == null) {
            return Collections.emptyMap();
        }
        return indexes.stream()
                .filter(idx -> namespace.equals(idx.getNamespace()))
                .collect(Collectors.groupingBy(Index::getBin));
    }
}
