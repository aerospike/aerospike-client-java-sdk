package com.aerospike.client.fluent.query;

import java.util.Collection;
import java.util.Set;

import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.ParticleType;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.Index;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParseResult;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.impl.DSLParserImpl;

public abstract class WhereClauseProcessor {
    protected final boolean allowsIndex;

    public abstract ParseResult process(String namespace, Session session);


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
        if (indexContext != null) {
            Collection<Index> indexes = indexContext.indexes();
            sb.append("{");
            for (Index index : indexes) {
                sb.append(index.getBinValuesRatio()).append(",");
            }
            sb.append("}");
        }
            ;
        return sb.toString();
    }

    public ParseResult process(String dsl, String namespace, Session session) {
        DSLParser parser = new DSLParserImpl();

        ParsedExpression parseResult;
        IndexContext indexContext = null;
        ExpressionContext context = ExpressionContext.of(dsl);
        if (allowsIndex) {
            Set<Index> indexes = session.getCluster().getIndexes();
            indexContext = IndexContext.of(namespace, indexes);
            parseResult = parser.parseExpression(context, indexContext);
        }
        else {
            parseResult = parser.parseExpression(context);
        }
        ParseResult result = parseResult.getResult();
        if (result.getExp() == null && result.getFilter() == null) {
            throw new DslParseException("Unknown error parsing DSL: '" + dsl + "'");
        }

        if (Log.debugEnabled()) {
            if (allowsIndex && result.getFilter() != null) {
                Filter filter = result.getFilter();

                Log.debug(String.format("Dsl('%s', '%s') => (Exp: %s, Filter: %s)",
                        dsl,
                        namespace,
                        result.getExp(),
                        formStringOfFilter(filter, indexContext)));
            }
            else {
                Log.debug(String.format("Dsl('%s', '%s') => (Exp: %s)",
                        dsl,
                        namespace,
                        result.getExp()));
            }
        }

        return result;
    }

    private static class WhereStringImpl extends WhereClauseProcessor {
        private final String dsl;
        public WhereStringImpl(boolean allowsIndex, String dsl) {
            super(allowsIndex);
            this.dsl = dsl;
        }

        @Override
        public ParseResult process(String namespace, Session session) {
            return process(this.dsl, namespace, session);
        }
    }

    private static class WherePreparedImpl extends WhereClauseProcessor {
        private final PreparedDsl dsl;
        private final Object[] params;
        public WherePreparedImpl(boolean allowsIndex, PreparedDsl dsl, Object... params) {
            super(allowsIndex);
            this.dsl = dsl;
            this.params = params;
        }

        @Override
        public ParseResult process(String namespace, Session session) {
            // TODO: For now, until DSL supports prepared statements
            String dslStr = dsl.formValue(params);
            return process(dslStr, namespace,  session);
        }
    }

    private static class WhereBoolExprImpl extends WhereClauseProcessor {
        private final BooleanExpression dsl;
        public WhereBoolExprImpl(boolean allowsIndex, BooleanExpression dsl) {
            super(allowsIndex);
            this.dsl = dsl;
        }

        @Override
        public ParseResult process(String namespace, Session session) {
             return new ParseResult(null, dsl.toAerospikeExp());
        }
    }

    private static class WhereExpImpl extends WhereClauseProcessor {
        private final Exp dsl;
        public WhereExpImpl(boolean allowsIndex, Exp dsl) {
            super(allowsIndex);
            this.dsl = dsl;
        }

        @Override
        public ParseResult process(String namespace, Session session) {
            return new ParseResult(null, dsl);
        }
    }

    public static WhereClauseProcessor from(boolean allowsIndex, String dsl) {
        return new WhereStringImpl(allowsIndex, dsl);
    }
    public static WhereClauseProcessor from(boolean allowsIndex, PreparedDsl dsl, Object ... params) {
        return new WherePreparedImpl(allowsIndex, dsl, params);
    }
    public static WhereClauseProcessor from(BooleanExpression dsl) {
        return new WhereBoolExprImpl(false, dsl);
    }
    public static WhereClauseProcessor from(Exp dsl) {
        return new WhereExpImpl(false, dsl);
    }
}