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

import com.aerospike.client.sdk.AelMaterializer;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;

/**
 * Holds a WHERE clause (string AEL, prepared AEL, {@link Exp}, or {@link BooleanExpression})
 * and materializes it to server-compiled filter bytes. No client-side AEL parsing or index selection.
 */
public abstract class WhereClauseProcessor {

    /**
     * AEL source text when this WHERE was built from a string or {@link PreparedAel}.
     */
    abstract String getAelString();

    /**
     * Whether this WHERE was built from an AEL string or {@link PreparedAel}.
     */
    public abstract boolean hasStringAel();

    /**
     * Materialize row filter expression bytes for field {@code 43} paths.
     */
    public abstract Exp toFilterExp(Cluster cluster);

    public final Exp toFilterExp(Session session) {
        return toFilterExp(session.getCluster());
    }

    public final Expression toFilterExpression(Session session) {
        return toFilterExpression(session.getCluster());
    }

    public final Expression toFilterExpression(Cluster cluster) {
        Exp exp = toFilterExp(cluster);
        return exp != null ? Exp.build(exp) : null;
    }

    private static final class WhereStringImpl extends WhereClauseProcessor {
        private final String ael;

        WhereStringImpl(String ael) {
            this.ael = ael;
        }

        @Override
        String getAelString() {
            return ael;
        }

        @Override
        public boolean hasStringAel() {
            return true;
        }

        @Override
        public Exp toFilterExp(Cluster cluster) {
            return Exp.expr(AelMaterializer.expressionFromString(cluster, ael));
        }
    }

    private static final class WherePreparedImpl extends WhereClauseProcessor {
        private final PreparedAel ael;
        private final Object[] params;

        WherePreparedImpl(PreparedAel ael, Object... params) {
            this.ael = ael;
            this.params = params;
        }

        @Override
        String getAelString() {
            return ael.formValue(params);
        }

        @Override
        public boolean hasStringAel() {
            return true;
        }

        @Override
        public Exp toFilterExp(Cluster cluster) {
            return Exp.expr(AelMaterializer.expressionFromPrepared(cluster, ael, params));
        }
    }

    private static final class WhereBoolExprImpl extends WhereClauseProcessor {
        private final BooleanExpression ael;

        WhereBoolExprImpl(BooleanExpression ael) {
            this.ael = ael;
        }

        @Override
        String getAelString() {
            throw new IllegalStateException("WHERE clause does not provide an AEL string");
        }

        @Override
        public boolean hasStringAel() {
            return false;
        }

        @Override
        public Exp toFilterExp(Cluster cluster) {
            return ael.toAerospikeExp();
        }
    }

    private static final class WhereExpImpl extends WhereClauseProcessor {
        private final Exp exp;

        WhereExpImpl(Exp exp) {
            this.exp = exp;
        }

        @Override
        String getAelString() {
            throw new IllegalStateException("WHERE clause does not provide an AEL string");
        }

        @Override
        public boolean hasStringAel() {
            return false;
        }

        @Override
        public Exp toFilterExp(Cluster cluster) {
            return exp;
        }
    }

    public static WhereClauseProcessor from(String ael) {
        return new WhereStringImpl(ael);
    }

    public static WhereClauseProcessor from(PreparedAel ael, Object... params) {
        return new WherePreparedImpl(ael, params);
    }

    public static WhereClauseProcessor from(BooleanExpression ael) {
        return new WhereBoolExprImpl(ael);
    }

    public static WhereClauseProcessor from(Exp exp) {
        return new WhereExpImpl(exp);
    }

    public static WhereClauseProcessor from(Expression exp) {
        return from(Exp.expr(exp));
    }
}
