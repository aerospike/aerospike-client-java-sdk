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
package com.aerospike.client.sdk;

import java.util.Collection;
import java.util.Set;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.Index;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.ParseResult;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.api.AelParser;
import com.aerospike.ael.impl.AelParserImpl;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.query.PreparedAel;
import com.aerospike.client.sdk.command.ParticleType;

public final class AelMaterializer {

    private AelMaterializer() {
    }

    /**
     * String AEL for filter/read/write ops: server-compiled payload when supported, else client parse.
     */
    public static Expression expressionFromString(Cluster cluster, String ael) {
        if (cluster.supportsAel()) {
            return Expression.fromServerCompiledFilter(ael);
        }
        return clientParseStringToExpression(ael);
    }

    public static Expression expressionFromString(Cluster cluster, String ael, Object[] params) {
        return expressionFromString(cluster, formatAel(ael, params));
    }

    public static Expression expressionFromPrepared(Cluster cluster, PreparedAel ael, Object[] params) {
        return expressionFromString(cluster, ael.formValue(params));
    }

    /**
     * Query WHERE from string AEL: server path when SI-aware parse is not required and cluster
     * supports server AEL; otherwise full client parse (including secondary index {@link Filter}).
     */
    public static ParseResult parseWhereFromString(
        Session session,
        boolean allowsIndex,
        String namespace,
        String querySet,
        String ael
    ) {
        if (!allowsIndex && session.getCluster().supportsAel()) {
            return serverCompiledFilterResult(ael);
        }
        return clientParseWhere(session, allowsIndex, namespace, querySet, ael);
    }

    private static ParseResult serverCompiledFilterResult(String dslSource) {
        return new ParseResult(null, Exp.expr(Expression.fromServerCompiledFilter(dslSource)));
    }

    private static ParseResult clientParseWhere(
        Session session,
        boolean allowsIndex,
        String namespace,
        String querySet,
        String ael
    ) {
        AelParser parser = new AelParserImpl();

        ParsedExpression parseResult;
        IndexContext indexContext = null;
        ExpressionContext context = ExpressionContext.of(ael);
        if (allowsIndex) {
            Set<Index> indexes = session.getCluster().getIndexes();
            indexContext = IndexContext.withQuerySet(namespace, querySet, indexes);
            parseResult = parser.parseExpression(context, indexContext);
        }
        else {
            parseResult = parser.parseExpression(context);
        }
        ParseResult result = parseResult.getResult();
        if (result.getExp() == null && result.getFilter() == null) {
            throw new AelParseException("Unknown error parsing AEL: '" + ael + "'");
        }

        if (Log.debugEnabled()) {
            if (allowsIndex && result.getFilter() != null) {
                Filter filter = result.getFilter();

                Log.debug(String.format("Ael('%s', '%s') => (Exp: %s, Filter: %s)",
                        ael,
                        namespace,
                        result.getExp(),
                        formStringOfFilter(filter, indexContext)));
            }
            else {
                Log.debug(String.format("Ael('%s', '%s') => (Exp: %s)",
                        ael,
                        namespace,
                        result.getExp()));
            }
        }

        return result;
    }

    private static Expression clientParseStringToExpression(String ael) {
        AelParser parser = new AelParserImpl();
        ExpressionContext context = ExpressionContext.of(ael);
        ParsedExpression parseResult = parser.parseExpression(context);
        Exp exp = parseResult.getResult().getExp();

        if (Log.debugEnabled()) {
            Log.debug(String.format("Ael(\"%s\") => (Exp: %s)",
                    ael,
                    exp));
        }

        return Exp.build(exp);
    }

    private static String valTypeToString(int type) {
        switch (type) {
        case ParticleType.BLOB:
            return "BLOB";
        case ParticleType.GEOJSON:
            return "GeoJSON";
        case ParticleType.INTEGER:
            return "numeric";
        case ParticleType.STRING:
            return "string";
        default:
            return "Unknown(" + type + ")";
        }
    }

    private static String shorten(Value value) {
        String val = value.toString();
        if (val.length() <= 8) {
            return val;
        }
        return val.substring(0, 5) + "...";
    }

    private static String filterCriteriaToString(Filter filter) {
        if (filter.getEnd() != null) {
            return "(" + shorten(filter.getBegin()) + "-" + shorten(filter.getEnd());
        }
        else {
            return "(" + shorten(filter.getBegin()) + ")";
        }
    }

    private static String formStringOfFilter(Filter filter, IndexContext indexContext) {
        StringBuffer sb = new StringBuffer();
        sb.append(filter.getName())
                .append(" [")
                .append(valTypeToString(filter.getValType()))
                .append(" ] ")
                .append(filterCriteriaToString(filter));
        if (indexContext != null && indexContext.getIndexes() != null) {
            Collection<Index> indexes = indexContext.getIndexes();
            sb.append("{");
            for (Index index : indexes) {
                sb.append(index.getBinValuesRatio()).append(",");
            }
            sb.append("}");
        }
        return sb.toString();
    }

    private static String formatAel(String ael, Object[] params) {
        if (params == null || params.length == 0) {
            return ael;
        }
        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        for (int i = 0; i < ael.length(); i++) {
            char c = ael.charAt(i);
            if (c == '?' && paramIndex < params.length) {
                result.append(formatParam(params[paramIndex++]));
            }
            else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static String formatParam(Object param) {
        if (param == null) {
            return "null";
        }
        else if (param instanceof String) {
            return "\"" + param + "\"";
        }
        else if (param instanceof Number || param instanceof Boolean) {
            return param.toString();
        }
        else {
            return "\"" + param.toString() + "\"";
        }
    }
}
