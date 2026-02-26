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
package com.aerospike.client.fluent.query;

import com.aerospike.client.fluent.ErrorHandler;
import com.aerospike.client.fluent.ErrorStrategy;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;

abstract class QueryImpl {
    private final Session session;
    private final QueryBuilder queryBuilder;

    public QueryImpl(QueryBuilder builder, Session session) {
        this.session = session;
        this.queryBuilder = builder;
    }
    public abstract RecordStream execute();
    public abstract RecordStream execute(ErrorStrategy strategy);
    public abstract RecordStream execute(ErrorHandler handler);
    public abstract RecordStream executeAsync(ErrorStrategy strategy);
    public abstract RecordStream executeAsync(ErrorHandler handler);
    public abstract boolean allowsSecondaryIndexQuery();


    public Session getSession() {
        return session;
    }

    protected QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public boolean hasPartitionFilter() {
        return queryBuilder.getStartPartition() > 0 || queryBuilder.getEndPartition() < 4096;
    }
}
