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
package com.aerospike.client.sdk.query;

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
import com.aerospike.client.sdk.Log;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.command.ParticleType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;

public abstract class WhereClauseProcessor {
    protected final boolean allowsIndex;

    /**
     * Parse AEL with no query-set filtering of secondary indexes (legacy behavior).
     */
    public final ParseResult process(String namespace, Session session) {
        return process(namespace, null, session);
    }

    /**
     * Parse AEL; when {@code querySet} is non-null and non-blank, only indexes whose set matches
     * (or have no set) participate in secondary-index selection.
     */
    public abstract ParseResult process(String namespace, String querySet, Session session);

    public WhereClauseProcessor(boolean allowsIndex) {
        this.allowsIndex = allowsIndex;
    }

    protected String valTypeToString(int type) {
        switch(type) {
        case ParticleType.BLOB: return "BLOB";
        case ParticleType.GEOJSON: return "GeoJSON";
        case ParticleType.INTEGER: return "numeric";
        case ParticleType.STRING: return "string";
        default: return "Unknown(" + type + ")";
        }
    }
    protected String shorten(Value value) {
        String val = value.toString();
        if (val.length() <= 8 ) {
            return val;
        }
        return val.substring(0, 5) + "...";
    }
    protected String filterCriteriaToString(Filter filter) {
        if (filter.getEnd() != null) {
            return "(" + shorten(filter.getBegin()) + "-" + shorten(filter.getEnd());
        }
        else {
            return "(" + shorten(filter.getBegin()) + ")";
        }
    }

    protected String formStringOfFilter(Filter filter, IndexContext indexContext) {
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
            ;
        return sb.toString();
    }

    protected ParseResult process(String ael, String namespace, String querySet, Session session) {
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

    private static ParseResult serverCompiledFilterResult(String dslSource) {
        return new ParseResult(null, Exp.expr(Expression.fromServerCompiledFilter(dslSource)));
    }

    /**
     * Without {@code allowsIndex} and with server AEL wire support, skip client parsing and use
     * {@link Expression#fromServerCompiledFilter}. With {@code allowsIndex}, client-parse first for SI;
     */
    protected final ParseResult processAel(
        String dslSource,
        String namespace,
        String querySet,
        Session session
    ) {
        if (!allowsIndex && session.getCluster().supportsServerCompiledFilterExpression()) {
            return serverCompiledFilterResult(dslSource);
        }
        return process(dslSource, namespace, querySet, session);
    }

    private static class WhereStringImpl extends WhereClauseProcessor {
        private final String ael;
        public WhereStringImpl(boolean allowsIndex, String ael) {
            super(allowsIndex);
            this.ael = ael;
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            return processAel(this.ael, namespace, querySet, session);
        }
    }

    private static class WherePreparedImpl extends WhereClauseProcessor {
        private final PreparedAel ael;
        private final Object[] params;
        public WherePreparedImpl(boolean allowsIndex, PreparedAel ael, Object... params) {
            super(allowsIndex);
            this.ael = ael;
            this.params = params;
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            // TODO: For now, until AEL supports prepared statements
            String aelStr = ael.formValue(params);
            return processAel(aelStr, namespace, querySet, session);
        }
    }

    private static class WhereBoolExprImpl extends WhereClauseProcessor {
        private final BooleanExpression ael;
        public WhereBoolExprImpl(boolean allowsIndex, BooleanExpression ael) {
            super(allowsIndex);
            this.ael = ael;
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            // namespace, querySet, session intentionally ignored - not required in this implementation
             return new ParseResult(null, ael.toAerospikeExp());
        }
    }

    private static class WhereExpImpl extends WhereClauseProcessor {
        private final Exp exp;
        public WhereExpImpl(boolean allowsIndex, Exp exp) {
            super(allowsIndex);
            this.exp = exp;
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            // namespace, querySet, session intentionally ignored - not required in this implementation
            return new ParseResult(null, exp);
        }
    }

    public static WhereClauseProcessor from(boolean allowsIndex, String ael) {
        return new WhereStringImpl(allowsIndex, ael);
    }
    public static WhereClauseProcessor from(boolean allowsIndex, PreparedAel ael, Object ... params) {
        return new WherePreparedImpl(allowsIndex, ael, params);
    }
    public static WhereClauseProcessor from(BooleanExpression ael) {
        return new WhereBoolExprImpl(false, ael);
    }
    public static WhereClauseProcessor from(Exp exp) {
        return new WhereExpImpl(false, exp);
    }
    public static WhereClauseProcessor from(Expression exp) {
        return from(Exp.expr(exp));
    }
}
