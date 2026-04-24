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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.sdk.command.Batch;
import com.aerospike.client.sdk.command.BatchAttr;
import com.aerospike.client.sdk.command.BatchCommand;
import com.aerospike.client.sdk.command.BatchDelete;
import com.aerospike.client.sdk.command.BatchExecutor;
import com.aerospike.client.sdk.command.BatchNode;
import com.aerospike.client.sdk.command.BatchNodes;
import com.aerospike.client.sdk.command.BatchRead;
import com.aerospike.client.sdk.command.BatchRecord;
import com.aerospike.client.sdk.command.BatchSingle;
import com.aerospike.client.sdk.command.BatchStatus;
import com.aerospike.client.sdk.command.BatchUDF;
import com.aerospike.client.sdk.command.BatchWrite;
import com.aerospike.client.sdk.command.DeleteExecutor;
import com.aerospike.client.sdk.command.ExistsExecutor;
import com.aerospike.client.sdk.command.IBatchCommand;
import com.aerospike.client.sdk.command.OperateArgs;
import com.aerospike.client.sdk.command.OperateReadCommand;
import com.aerospike.client.sdk.command.OperateReadExecutor;
import com.aerospike.client.sdk.command.OperateWriteCommand;
import com.aerospike.client.sdk.command.OperateWriteExecutor;
import com.aerospike.client.sdk.command.ReadAttr;
import com.aerospike.client.sdk.command.ReadCommand;
import com.aerospike.client.sdk.command.ReadExecutor;
import com.aerospike.client.sdk.command.TouchExecutor;
import com.aerospike.client.sdk.command.Txn;
import com.aerospike.client.sdk.command.TxnMonitor;
import com.aerospike.client.sdk.command.UdfCommand;
import com.aerospike.client.sdk.command.UdfExecutor;
import com.aerospike.client.sdk.command.WriteCommand;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.tend.Partitions;

/**
 * Executor for heterogeneous batch operations.
 * This class converts OperationSpec objects to the appropriate BatchRecord types
 * and executes them as a single batch operation.
 *
 * <p>The executor handles mixed operation types including:
 * <ul>
 *   <li>Write operations (upsert, update, insert, replace) - converted to BatchWrite</li>
 *   <li>Delete operations - converted to BatchDelete</li>
 *   <li>Touch operations - converted to BatchWrite with Operation.touch()</li>
 *   <li>Exists operations - converted to BatchRead with no bins</li>
 *   <li>Query/read operations - converted to BatchRead with specified bins</li>
 * </ul>
 * </p>
 */
class OperationSpecExecutor {

    /**
     * Resolve the TTL for an operation spec.
     * Resolution order: spec explicit > chain default > server default (0).
     */
    private static long resolveTtl(OperationSpec spec, long defaultExpirationInSeconds) {
        if (spec.hasExplicitExpiration()) {
            return spec.getExpirationInSeconds();
        }
        if (defaultExpirationInSeconds != AbstractOperationBuilder.NOT_EXPLICITLY_SET) {
            return defaultExpirationInSeconds;
        }
        return AbstractOperationBuilder.TTL_SERVER_DEFAULT;
    }

    /**
     * Execute a batch of heterogeneous operations with errors embedded in the stream.
     * Used by async paths where error routing is handled at the builder level.
     */
    public static RecordStream execute(
        Session session, List<OperationSpec> specs, Expression defaultWhereClause,
        long defaultExpirationInSeconds, Txn txn, boolean notInAnyTransaction,
        Boolean durableDeleteDefault
    ) {
        return execute(session, specs, defaultWhereClause, defaultExpirationInSeconds, txn,
            notInAnyTransaction, ErrorDisposition.IN_STREAM, durableDeleteDefault);
    }

    /**
     * Execute a batch of heterogeneous operations with the given error disposition.
     *
     * @param session the session to use for execution
     * @param specs the list of operation specifications
     * @param defaultWhereClause optional default filter for operations without explicit where clause
     * @param defaultExpirationInSeconds default TTL for operations without explicit expiration (NOT_EXPLICITLY_SET if not set)
     * @param txn optional transaction to use
     * @param notInAnyTransaction disallow implicit transactions
     * @param disposition how to handle per-record errors (throw, embed in stream, or dispatch to handler)
     * @return RecordStream containing the results of all operations
     */
    public static RecordStream execute(
        Session session, List<OperationSpec> specs, Expression defaultWhereClause,
        long defaultExpirationInSeconds, Txn txn, boolean notInAnyTransaction,
        ErrorDisposition disposition, Boolean durableDeleteDefault
    ) {
        if (specs.isEmpty()) {
            return new RecordStream();
        }

        // Single-key optimization: bypass batch infrastructure for single-spec, single-key operations
        if (specs.size() == 1) {
            OperationSpec spec = specs.get(0);
            List<Key> keys = spec.getKeys();

            if (keys.size() == 1) {
                Key key = keys.get(0);
                Expression filterExp = spec.getWhereClause() != null ?
                    spec.getWhereClause() : defaultWhereClause;

                return executeSingleKey(session, spec, key, filterExp, defaultExpirationInSeconds,
                    txn, disposition, durableDeleteDefault);
            }
        }

        // TODO Track count of keys in builders, so it can be used here.
        List<BatchRecord> records = new ArrayList<>(512);
        Cluster cluster = session.getCluster();
        Behavior behavior = session.getBehavior();
        // TODO: Put in hashmap
        BatchAttr attr = new BatchAttr();
        BatchStatus status = new BatchStatus();
        AerospikeException except = null;
        boolean includeMissingKeys = false;
        boolean failOnFilteredOut = false;
        Mode mode = Mode.AP;
        boolean linearize = false;
        boolean hasWrite = false;

        for (OperationSpec spec : specs) {
            // Set isIncludeMissingKeys if any spec has isIncludeMissingKeys.
            if (spec.isIncludeMissingKeys()) {
                includeMissingKeys = true;
            }

            if (spec.isFailOnFilteredOut()) {
                failOnFilteredOut = true;
            }

            // Create BatchRecord(s) for each key in this spec
            for (Key key : spec.getKeys()) {
                Partitions partitions;

                try {
                    partitions = cluster.getPartitions(key.namespace);
                }
                catch (AerospikeException ae) {
                    // Create record and set to error state.
                    BatchRecord rec = new BatchRecord(key, false);
                    rec.setError(ae.getResultCode(), false);
                    records.add(rec);

                    if (except == null) {
                        except = ae;
                    }
                    continue;
                }

                boolean scMode = partitions.scMode;
                BatchRecord rec;

                if (spec.isQuery()) {
                    // Query (read) operation
                    ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, scMode);
                    int ttl = settings.getResetTtlOnReadAtPercent();

                    attr.setRead(settings, scMode);

                    if (attr.linearize) {
                        linearize = true;
                    }

                    List<Operation> ops = spec.getOperations();
                    if (ops != null && !ops.isEmpty()) {
                        // Use operations for CDT reads, selectFrom, etc.
                        rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, ops);
                    }
                    else if (spec.getProjectedBins() != null) {
                        if (spec.getProjectedBins().length > 0) {
                            // Read specific bins
                            rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, spec.getProjectedBins());
                        } else {
                            // Empty array = withNoBins (header only)
                            rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, false);
                        }
                    }
                    else {
                        // null = Read all bins (default)
                        rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, true);
                    }
                }
                else if (spec.getOpType() == OpType.EXISTS) {
                    ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, scMode);
                    int ttl = settings.getResetTtlOnReadAtPercent();

                    attr.setRead(settings, scMode);

                    if (attr.linearize) {
                        linearize = true;
                    }

                    rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, false);
                }
                else {
                    ResolvedSettings settings = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, scMode);
                    hasWrite = true;

                    switch (spec.getOpType()) {
                    case UPSERT:
                    case UPDATE:
                    case INSERT:
                    case REPLACE:
                    case REPLACE_IF_EXISTS:
                        attr.setWrite(settings, spec.getOpType(), durableDeleteDefault, spec.getDurableDelete());
                        rec = new BatchWrite(key, attr, spec);
                        break;

                    case TOUCH:
                        attr.setTouch(settings);
                        rec = new BatchWrite(key, attr, spec, List.of(Operation.touch()), OpType.TOUCH);
                        break;

                    case DELETE:
                        attr.setDelete(settings, durableDeleteDefault, spec.getDurableDelete());
                        rec = new BatchDelete(key, attr, spec);
                        break;

                    case UDF:
                        attr.setUDF(settings, spec.getOpType(), durableDeleteDefault, spec.getDurableDelete());
                        Expression udfWhereClause = spec.getWhereClause() != null ? spec.getWhereClause() : defaultWhereClause;
                        Value[] udfArgs = spec.getUdfArguments();
                        int udfTtl = (int) resolveTtl(spec, defaultExpirationInSeconds);
                        rec = new BatchUDF(key, udfWhereClause, attr, spec.getUdfPackageName(),
                            spec.getUdfFunctionName(), udfArgs != null ? udfArgs : new Value[0], udfTtl);
                        break;

                    default:
                        throw new IllegalStateException("Unknown operation type: " + spec.getOpType());
                    }
                }

                if (scMode) {
                    mode = Mode.CP;
                }

                records.add(rec);
            }
        }

        if (except != null) {
            // Fatal if no key requests were generated on initialization.
            if (records.size() == 0) {
                throw except;
            }
            else {
                status.batchKeyError(except);
            }
        }

        OpKind kind = hasWrite? OpKind.WRITE_NON_RETRYABLE : OpKind.READ;
        ResolvedSettings settings = behavior.getSettings(kind, OpShape.BATCH, mode);

        BatchCommand parent = new BatchCommand(cluster, null, txn, null, records,
            defaultWhereClause, includeMissingKeys, failOnFilteredOut, linearize, settings);

        List<BatchNode> bns = BatchNodes.generate(cluster, parent, records, status);

        IBatchCommand[] commands = new IBatchCommand[bns.size()];
        int count = 0;

        for (BatchNode bn : bns) {
            if (bn.offsetsSize == 1) {
                int i = bn.offsets[0];
                BatchRecord rec = records.get(i);

                switch (rec.getType()) {
                case BATCH_READ:
                    commands[count++] = new BatchSingle.ReadRecordSync(cluster, parent,
                        (BatchRead)rec, status, bn.node);
                    break;

                case BATCH_WRITE:
                    commands[count++] = new BatchSingle.OperateRecordSync(cluster, parent,
                        (BatchWrite)rec, status, bn.node);
                    break;

                case BATCH_UDF:
                    commands[count++] = new BatchSingle.Udf(cluster, parent, (BatchUDF)rec, status,
                        bn.node);
                    break;

                case BATCH_DELETE:
                    commands[count++] = new BatchSingle.Delete(cluster, parent,
                        (BatchDelete)rec, status, bn.node);
                    break;
               }
            }
            else {
                commands[count++] = new Batch.OperateListSync(cluster, parent, bn, records, status);
            }
        }

        if (txn != null) {
            TxnMonitor.addKeysBatchReadWrite(txn, session, records);
            BatchExecutor.execute(cluster, commands, status);
        }
        else if (!notInAnyTransaction && hasWrite && mode == Mode.CP &&
            cluster.allowImplicitBatchWriteTransactions()) {
            // Create implicit transaction for the batch.
            session.doInTransaction(txnSession -> {
                TxnMonitor.addKeysBatchReadWrite(txnSession.getCurrentTransaction(), txnSession, records);
                BatchExecutor.execute(cluster, commands, status);
            });
        }
        else {
            BatchExecutor.execute(cluster, commands, status);
        }

        AsyncRecordStream recordStream = new AsyncRecordStream(records.size());

        try {
            for (int i = 0; i < records.size(); i++) {
                BatchRecord br = records.get(i);

                if (!shouldIncludeResult(br.resultCode, includeMissingKeys, failOnFilteredOut, br.hasWrite, true)) {
                    continue;
                }

                RecordResult result = AbstractFilterableBuilder.createRecordResultFromBatchRecord(br, settings, i);

                if (AbstractFilterableBuilder.isActionableError(br.resultCode)) {
                    switch (disposition) {
                        case ErrorDisposition.Throw ignored -> {
                            AerospikeException ex = result.exception() != null
                                ? result.exception()
                                : AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt);
                            throw ex;
                        }
                        case ErrorDisposition.Handler h ->
                            AbstractFilterableBuilder.dispatchError(result, h.errorHandler());
                        case ErrorDisposition.InStream ignored ->
                            recordStream.publish(result);
                    }
                } else {
                    recordStream.publish(result);
                }
            }
            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
    }

    /**
     * Build RecordStream from batch results, respecting includeMissingKeys and failOnFilteredOut flags.
     */
    /*
    private static RecordStream buildRecordStream(List<BatchRecord> batchRecords,
                                                   List<OperationSpec> specs,
                                                   Settings settings) {
        List<RecordResult> results = new ArrayList<>();

        int recordIndex = 0;

        for (OperationSpec spec : specs) {
            for (int keyIndex = 0; keyIndex < spec.getKeys().size(); keyIndex++) {
                BatchRecord br = batchRecords.get(recordIndex);

                // Determine if we should include this result
                boolean includeResult = shouldIncludeResult(br.resultCode, spec);

                if (includeResult) {
                    RecordResult result;
                    if (settings.getStackTraceOnException() && AbstractFilterableBuilder.isActionableError(br.resultCode)) {
                        result = new RecordResult(
                            br,
                            AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt),
                            recordIndex);
                    } else {
                        result = new RecordResult(br, recordIndex);
                    }
                    results.add(result);
                }

                recordIndex++;
            }
        }

        return new RecordStream(results, 0);
    }
*/
    /**
     * Determine if a result should be included based on result code and operation flags.
     */
/*
    private static boolean shouldIncludeResult(int resultCode, OperationSpec spec) {
        return shouldIncludeResult(resultCode, spec.isIncludeMissingKeys(), spec.isFailOnFilteredOut());
    }
*/
    /**
     * Determine if a result should be included based on result code, flags, and operation context.
     * FILTERED_OUT: batch writes always include; otherwise depends on failOnFilteredOut.
     * KEY_NOT_FOUND: writes always include; reads depend on includeMissingKeys.
     */
    private static boolean shouldIncludeResult(int resultCode, boolean includeMissingKeys,
            boolean failOnFilteredOut, boolean hasWrite, boolean isBatch) {
        switch (resultCode) {
        case ResultCode.OK:
            return true;
        case ResultCode.FILTERED_OUT:
            if (isBatch && hasWrite) {
                return true;
            }
            return failOnFilteredOut;
        case ResultCode.KEY_NOT_FOUND_ERROR:
            if (hasWrite) {
                return true;
            }
            return includeMissingKeys;
        default:
            return true;
        }
    }

    /**
     * Execute a single-key operation using direct point executors, bypassing batch infrastructure.
     * This is more efficient than going through BatchNodes, BatchRecord, and BatchExecutor.
     */
    private static RecordStream executeSingleKey(
        Session session, OperationSpec spec, Key key, Expression filterExp,
        long defaultExpirationInSeconds, Txn txn, ErrorDisposition disposition,
        Boolean durableDeleteDefault
    ) {
        Cluster cluster = session.getCluster();
        Behavior behavior = session.getBehavior();
        Partitions partitions;

        try {
            partitions = cluster.getPartitions(key.namespace);
        }
        catch (AerospikeException ae) {
            return handleSingleKeyError(key, ae, spec, disposition);
        }

        boolean scMode = partitions.scMode;
        boolean includeMissingKeys = spec.isIncludeMissingKeys();
        boolean failOnFilteredOut = spec.isFailOnFilteredOut();
        long ttl = resolveTtl(spec, defaultExpirationInSeconds);

        try {
            if (spec.isQuery()) {
                return executeSingleKeyRead(session, cluster, behavior, partitions, spec, key,
                    filterExp, txn, scMode, includeMissingKeys, failOnFilteredOut);
            }
            else if (spec.getOpType() == OpType.EXISTS) {
                return executeSingleKeyExists(session, cluster, behavior, partitions, spec, key,
                    filterExp, txn, scMode, failOnFilteredOut);
            }
            else if (spec.getOpType() == OpType.TOUCH) {
                return executeSingleKeyTouch(session, cluster, behavior, partitions, spec, key,
                    filterExp, ttl, txn, scMode, failOnFilteredOut);
            }
            else if (spec.getOpType() == OpType.DELETE) {
                return executeSingleKeyDelete(session, cluster, behavior, partitions, spec, key,
                    filterExp, txn, scMode, failOnFilteredOut, durableDeleteDefault);
            }
            else if (spec.getOpType() == OpType.UDF) {
                return executeSingleKeyUdf(session, cluster, behavior, partitions, spec, key,
                    filterExp, ttl, txn, scMode, includeMissingKeys, failOnFilteredOut,
                    durableDeleteDefault);
            }
            else {
                return executeSingleKeyWrite(session, cluster, behavior, partitions, spec, key,
                    filterExp, ttl, txn, scMode, includeMissingKeys, failOnFilteredOut,
                    durableDeleteDefault);
            }
        }
        catch (AerospikeException ae) {
            return handleSingleKeyError(key, ae, spec, disposition);
        }
    }

    private static RecordStream handleSingleKeyError(
        Key key, AerospikeException ae, OperationSpec spec, ErrorDisposition disposition
    ) {
        boolean hasWrite = spec.getOpType() != null;
        if (!shouldIncludeResult(ae.getResultCode(), spec.isIncludeMissingKeys(), spec.isFailOnFilteredOut(), hasWrite, false)) {
            return new RecordStream();
        }

        return switch (disposition) {
            case ErrorDisposition.Throw ignored -> throw ae;
            case ErrorDisposition.InStream ignored -> new RecordStream(new RecordResult(key, ae, 0));
            case ErrorDisposition.Handler h -> {
                h.errorHandler().handle(key, 0, ae);
                yield new RecordStream();
            }
        };
    }

    /**
     * Execute a single-key read operation using ReadExecutor or OperateReadExecutor.
     */
    private static RecordStream executeSingleKeyRead(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression filterExp, Txn txn,
        boolean scMode, boolean includeMissingKeys, boolean failOnFilteredOut
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, scMode);
        ReadAttr attr = new ReadAttr(partitions, settings);

        if (txn != null) {
            txn.prepareRead(key.namespace);
        }

        List<Operation> ops = spec.getOperations();
        Record rec;

        if (ops != null && !ops.isEmpty()) {
            OperateArgs operateArgs = new OperateArgs(ops);
            OperateReadCommand cmd = new OperateReadCommand(cluster, partitions, txn, key, ops, operateArgs,
                filterExp, failOnFilteredOut, settings, attr);
            OperateReadExecutor exec = new OperateReadExecutor(cluster, cmd);
            exec.execute();
            rec = exec.getRecord();
        }
        else {
            String[] binNames = spec.getProjectedBins();
            // withNoBins is true ONLY if explicitly set (empty array), not if null (null = read all bins)
            boolean withNoBins = binNames != null && binNames.length == 0;
            ReadCommand cmd = new ReadCommand(cluster, partitions, txn, key, binNames, withNoBins,
                filterExp, failOnFilteredOut, settings, attr);
            ReadExecutor exec = new ReadExecutor(cluster, cmd);
            exec.execute();
            rec = exec.getRecord();
        }

        return createRecordStream(key, rec, includeMissingKeys);
    }

    /**
     * Execute a single-key write operation using OperateWriteExecutor.
     */
    private static RecordStream executeSingleKeyWrite(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression filterExp, long ttl, Txn txn,
        boolean scMode, boolean includeMissingKeys, boolean failOnFilteredOut,
        Boolean durableDeleteDefault
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, scMode);
        int gen = spec.getGeneration();
        List<Operation> ops = spec.getOperations();
        OpType opType = spec.getOpType();

        if (txn != null) {
            TxnMonitor.addKey(txn, session, key);
        }

        OperateArgs operateArgs = new OperateArgs(ops);
        OperateWriteCommand cmd = new OperateWriteCommand(cluster, partitions, txn, key, ops, operateArgs,
            opType, gen, (int) ttl, filterExp, failOnFilteredOut, settings, durableDeleteDefault,
            spec.getDurableDelete());
        OperateWriteExecutor exec = new OperateWriteExecutor(cluster, cmd);
        exec.execute();
        Record rec = exec.getRecord();

        return createRecordStream(key, rec, includeMissingKeys);
    }

    private static RecordStream createRecordStream(Key key, Record rec, boolean includeMissingKeys) {
        if (rec != null) {
            return new RecordStream(key, rec);
        }
        else if (includeMissingKeys) {
            return new RecordStream(new RecordResult(key, ResultCode.KEY_NOT_FOUND_ERROR, false,
                ResultCode.getResultString(ResultCode.KEY_NOT_FOUND_ERROR), 0));
        }
        return new RecordStream();
    }

    /**
     * Execute a single-key exists operation using ExistsExecutor.
     */
    private static RecordStream executeSingleKeyExists(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression filterExp, Txn txn,
        boolean scMode, boolean failOnFilteredOut
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.READ, OpShape.POINT, scMode);
        ReadAttr attr = new ReadAttr(partitions, settings);

        if (txn != null) {
            txn.prepareRead(key.namespace);
        }

        ReadCommand cmd = new ReadCommand(cluster, partitions, txn, key, null, true,
            filterExp, failOnFilteredOut, settings, attr);
        ExistsExecutor exec = new ExistsExecutor(cluster, cmd);
        exec.execute();
        boolean exists = exec.exists();

        int resultCode = exists ? ResultCode.OK : ResultCode.KEY_NOT_FOUND_ERROR;
        return new RecordStream(new RecordResult(key, resultCode, false,
            ResultCode.getResultString(resultCode), 0));
    }

    /**
     * Execute a single-key touch operation using TouchExecutor.
     */
    private static RecordStream executeSingleKeyTouch(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression filterExp, long ttl, Txn txn,
        boolean scMode, boolean failOnFilteredOut
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, scMode);
        int gen = spec.getGeneration();

        if (txn != null) {
            TxnMonitor.addKey(txn, session, key);
        }

        WriteCommand cmd = new WriteCommand(cluster, partitions, txn, key, OpType.TOUCH,
            gen, (int) ttl, filterExp, failOnFilteredOut, settings);
        TouchExecutor exec = new TouchExecutor(cluster, cmd);
        exec.execute();
        boolean touched = exec.touched();

        int resultCode = touched ? ResultCode.OK : ResultCode.KEY_NOT_FOUND_ERROR;
        return new RecordStream(new RecordResult(key, resultCode, false,
            ResultCode.getResultString(resultCode), 0));
    }

    /**
     * Execute a single-key delete operation using DeleteExecutor.
     */
    private static RecordStream executeSingleKeyDelete(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression filterExp, Txn txn,
        boolean scMode, boolean failOnFilteredOut, Boolean durableDeleteDefault
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, scMode);
        int gen = spec.getGeneration();
        int ttl = (int) spec.getExpirationInSeconds();

        if (txn != null) {
            TxnMonitor.addKey(txn, session, key);
        }

        WriteCommand cmd = new WriteCommand(cluster, partitions, txn, key, OpType.DELETE,
            gen, ttl, filterExp, failOnFilteredOut, settings, durableDeleteDefault, spec.getDurableDelete());
        DeleteExecutor exec = new DeleteExecutor(cluster, cmd);
        exec.execute();
        boolean existed = exec.existed();

        int resultCode = existed ? ResultCode.OK : ResultCode.KEY_NOT_FOUND_ERROR;
        return new RecordStream(new RecordResult(key, resultCode, false,
            ResultCode.getResultString(resultCode), 0));
    }

    /**
     * Execute a single-key UDF operation.
     * UDF execution requires server-side Lua functions to be registered.
     */
    private static RecordStream executeSingleKeyUdf(
        Session session, Cluster cluster, Behavior behavior, Partitions partitions,
        OperationSpec spec, Key key, Expression where, long ttl, Txn txn,
        boolean scMode, boolean includeMissingKeys, boolean failOnFilteredOut,
        Boolean durableDeleteDefault
    ) {
        ResolvedSettings settings = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, scMode);

        if (txn != null) {
            TxnMonitor.addKey(txn, session, key);
        }

        UdfCommand cmd = new UdfCommand(cluster, partitions, txn, key, spec, (int)ttl, where,
            failOnFilteredOut, settings, durableDeleteDefault, spec.getDurableDelete());

        UdfExecutor exec = new UdfExecutor(cluster, cmd);
        exec.execute();

        Record rec = exec.getRecord();

        if (rec != null) {
            Object udfResult = extractUdfResult(rec);
            return new RecordStream(new RecordResult(key, udfResult, 0));
        }
        else if (includeMissingKeys) {
            return new RecordStream(new RecordResult(key, ResultCode.KEY_NOT_FOUND_ERROR, false,
                ResultCode.getResultString(ResultCode.KEY_NOT_FOUND_ERROR), 0));
        }

        return new RecordStream();
    }

    /**
     * Extract the UDF return value from a record.
     * UDFs typically return their result in a special bin.
     */
    private static Object extractUdfResult(Record record) {
        if (record == null || record.bins == null) {
            return null;
        }
        Object result = record.bins.get("SUCCESS");
        if (result != null) {
            return result;
        }
        Object failure = record.bins.get("FAILURE");
        if (failure != null) {
            throw AerospikeException.resultCodeToException(ResultCode.UDF_BAD_RESPONSE, "UDF execution failed: " + failure);
        }
        if (record.bins.size() == 1) {
            return record.bins.values().iterator().next();
        }
        return record.bins;
    }
}
