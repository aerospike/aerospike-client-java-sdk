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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.AsyncRecordStream;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.ErrorHandler;
import com.aerospike.client.sdk.ErrorStrategy;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;

public class IndexQueryBuilderImpl extends QueryImpl {
    private static final Logger log = LoggerFactory.getLogger(Loggers.QUERY);

    private final DataSet dataSet;

    public IndexQueryBuilderImpl(QueryBuilder builder, Session session, DataSet dataSet) {
        super(builder, session);
        this.dataSet = dataSet;
    }

    @Override
    public RecordStream execute() {
        return executeInternal(null);
    }

    @Override
    public RecordStream execute(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeInternal(null);
    }

    @Override
    public RecordStream execute(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return executeInternal(handler);
    }

    @Override
    public RecordStream executeAsync(ErrorStrategy strategy) {
        Objects.requireNonNull(strategy, "ErrorStrategy must not be null");
        return executeAsyncInternal(null);
    }

    @Override
    public RecordStream executeAsync(ErrorHandler handler) {
        Objects.requireNonNull(handler, "ErrorHandler must not be null");
        return executeAsyncInternal(handler);
    }

    private RecordStream executeInternal(ErrorHandler handler) {
        Session session = getSession();
        QueryBuilder qb = getQueryBuilder();

        validate(session, qb);

        ResolvedSettings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        WhereClauseProcessor where = qb.getAel();

        QueryCommand cmd = IndexProbePlanner.buildCommand(
            session, dataSet, where, qb.getQueryHint(), policy, qb);

        AsyncRecordStream stream = newStream(policy, handler);
        cmd.execute(stream);

        if (qb.getChunkSize() == 0) {
            // Normal query
            return new RecordStream(stream);

        }
        else {
            // Paginated query
            return new RecordStream(stream, cmd, qb.getLimit(), policy.getRecordQueueSize(), handler);
        }
    }

    /**
     * Asynchronous sibling of {@link #executeInternal(ErrorHandler)}: returns a stream immediately and
     * does the work on a virtual thread.
     *
     * <p>Planning an index query is a server round-trip (the explain probe), so leaving it in the calling
     * thread made {@code executeAsync} block for as long as the server took to answer - CLIENT-5523. Only
     * the argument checks stay here, because a caller that passed nonsense should hear about it from the
     * call rather than from the stream. Everything after that, planning included, happens on the virtual
     * thread, and a planning failure reaches the caller through the stream like any other query error.</p>
     */
    private RecordStream executeAsyncInternal(ErrorHandler handler) {
        Session session = getSession();
        Cluster cluster = session.getCluster();
        QueryBuilder qb = getQueryBuilder();

        validate(session, qb);

        ResolvedSettings policy = session.getBehavior().getSettings(OpKind.READ, OpShape.QUERY, Mode.ANY);
        WhereClauseProcessor where = qb.getAel();

        AsyncRecordStream stream = newStream(policy, handler);
        CompletableFuture<QueryCommand> planned = new CompletableFuture<>();

        cluster.startVirtualThread(() -> {
            QueryCommand cmd;

            try {
                cmd = IndexProbePlanner.buildCommand(
                    session, dataSet, where, qb.getQueryHint(), policy, qb);
            }
            catch (Throwable t) {
                planned.completeExceptionally(t);
                stream.error(t);
                return;
            }

            planned.complete(cmd);

            try {
                cmd.execute(stream);
            }
            catch (Throwable t) {
                stream.error(t);
            }
        });

        if (qb.getChunkSize() == 0) {
            // Normal query
            return new RecordStream(stream);
        }
        else {
            // Paginated query: the chunk loader needs the planned command, which is not ready yet.
            return new RecordStream(stream, () -> awaitPlan(planned), qb.getLimit(),
                policy.getRecordQueueSize(), handler);
        }
    }

    /** Cheap, local checks that belong to the call itself rather than to the stream it returns. */
    private void validate(Session session, QueryBuilder qb) {
        Cluster cluster = session.getCluster();

        warnQueryDoesNotParticipateInTransaction(qb);

        // Check for operations - not supported on servers < 8.1.2
        if (!cluster.supportsQueryOperations() && qb.getOperations() != null &&
            !qb.getOperations().isEmpty()) {
            throw AerospikeException.toException(ResultCode.OP_NOT_APPLICABLE,
                "Index query with read operations requires server version 8.1.2+. Server version is " +
                cluster.getVersion());
        }
    }

    private static AsyncRecordStream newStream(ResolvedSettings policy, ErrorHandler handler) {
        AsyncRecordStream stream = new AsyncRecordStream(policy.getRecordQueueSize());

        if (handler != null) {
            stream.withErrorHandler(handler);
        }
        return stream;
    }

    /**
     * Waits for the plan produced on the virtual thread, unwrapping the completion wrapper so a planning
     * failure surfaces as the {@link AerospikeException} the caller would have seen synchronously.
     */
    private static QueryCommand awaitPlan(CompletableFuture<QueryCommand> planned) {
        try {
            return planned.join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    /**
     * Reports that this query will not take part in the transaction it is running inside.
     *
     * <p>The server has no multi-record transaction support on the query path, so a query is always evaluated
     * outside any transaction, whether the transaction was passed explicitly or inherited from the session.
     * The consequence is a disagreement rather than an error: within one transaction the same record reads as
     * two different values depending on how it is asked for, because a point read participates and sees the
     * transaction's own writes while a query does not and still sees the pre-transaction state. Rows a query
     * returns are also absent from the transaction's read set, so commit cannot detect that another writer
     * changed them.</p>
     *
     * <p>None of that is new behaviour; it was simply silent. Warning rather than failing is deliberate,
     * because querying for keys inside a transactional block and then writing those keys transactionally is
     * legitimate and has to keep working. Whether the explicit case should instead be refused outright is
     * still open (CLIENT-5404).</p>
     */
    private static void warnQueryDoesNotParticipateInTransaction(QueryBuilder qb) {
        if (qb.getTxnToUse() == null || !log.isWarnEnabled()) {
            return;
        }

        log.warn(
            "Query executed inside a transaction will not take part in it. The server has no multi-record "
                + "transaction support on the query path, so this query reads the state as it was before the "
                + "transaction began: it will not see the transaction's own writes, and the rows it returns "
                + "are not protected against concurrent modification at commit. Call notInAnyTransaction() "
                + "on this query to confirm that is intended and silence this warning.");
    }
}
