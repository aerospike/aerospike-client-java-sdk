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

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.AelMaterializer;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.ael.BooleanExpression;
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

    /**
     * AEL source text when this WHERE was built from a string or {@link PreparedAel}.
     */
    abstract String getAelString();

    protected WhereClauseProcessor(boolean allowsIndex, boolean hasStringAel) {
        this.allowsIndex = allowsIndex;
        this.hasStringAel = hasStringAel;
    }

    private final boolean hasStringAel;

    /**
     * Whether this WHERE may participate in secondary-index query planning (AEL string / prepared).
     */
    public final boolean allowsIndex() {
        return allowsIndex;
    }

    /**
     * Whether this WHERE was built from an AEL string or {@link PreparedAel}.
     */
    public final boolean hasStringAel() {
        return hasStringAel;
    }

    private static class WhereStringImpl extends WhereClauseProcessor {
        private final String ael;
        public WhereStringImpl(boolean allowsIndex, String ael) {
            super(allowsIndex, true);
            this.ael = ael;
        }

        @Override
        String getAelString() {
            return ael;
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            return AelMaterializer.parseWhereFromString(session, this.ael);
        }
    }

    private static class WherePreparedImpl extends WhereClauseProcessor {
        private final PreparedAel ael;
        private final Object[] params;
        public WherePreparedImpl(boolean allowsIndex, PreparedAel ael, Object... params) {
            super(allowsIndex, true);
            this.ael = ael;
            this.params = params;
        }

        @Override
        String getAelString() {
            return ael.formValue(params);
        }

        @Override
        public ParseResult process(String namespace, String querySet, Session session) {
            String aelStr = getAelString();
            return AelMaterializer.parseWhereFromString(session, aelStr);
        }
    }

    private static class WhereBoolExprImpl extends WhereClauseProcessor {
        private final BooleanExpression ael;
        public WhereBoolExprImpl(boolean allowsIndex, BooleanExpression ael) {
            super(allowsIndex, false);
            this.ael = ael;
        }

        @Override
        String getAelString() {
            throw new IllegalStateException("WHERE clause does not provide an AEL string");
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
            super(allowsIndex, false);
            this.exp = exp;
        }

        @Override
        String getAelString() {
            throw new IllegalStateException("WHERE clause does not provide an AEL string");
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
