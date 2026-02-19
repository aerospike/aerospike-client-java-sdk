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
package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.command.Batch;
import com.aerospike.client.fluent.command.BatchAttr;
import com.aerospike.client.fluent.command.BatchCommand;
import com.aerospike.client.fluent.command.BatchDelete;
import com.aerospike.client.fluent.command.BatchExecutor;
import com.aerospike.client.fluent.command.BatchNode;
import com.aerospike.client.fluent.command.BatchNodes;
import com.aerospike.client.fluent.command.BatchRead;
import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.BatchSingle;
import com.aerospike.client.fluent.command.BatchStatus;
import com.aerospike.client.fluent.command.BatchWrite;
import com.aerospike.client.fluent.command.IBatchCommand;
import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnMonitor;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Behavior.Mode;
import com.aerospike.client.fluent.policy.Behavior.OpKind;
import com.aerospike.client.fluent.policy.Behavior.OpShape;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.tend.Partitions;

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
     * Execute a batch of heterogeneous operations.
     *
     * @param session the session to use for execution
     * @param specs the list of operation specifications
     * @param defaultWhereClause optional default filter for operations without explicit where clause
     * @param txn optional transaction to use
     * @return RecordStream containing the results of all operations
     */
    public static RecordStream execute(
    	Session session, List<OperationSpec> specs, Expression defaultWhereClause, Txn txn
    ) {
        if (specs.isEmpty()) {
            return new RecordStream();
        }

        // TODO Track count of keys in builders, so it can be used here.
        List<BatchRecord> records = new ArrayList<>(512);
        Cluster cluster = session.getCluster();
        Behavior behavior = session.getBehavior();
        // TODO: Put in hashmap
        BatchAttr attr = new BatchAttr();
        BatchStatus status = new BatchStatus();
		AerospikeException except = null;
        boolean respondAllKeys = false;
        boolean failOnFilteredOut = false;
        OpKind kind = OpKind.READ;
        Mode mode = Mode.AP;
        boolean linearize = false;

        for (OperationSpec spec : specs) {
            // Set isRespondAllKeys if any spec has isRespondAllKeys.
            if (spec.isRespondAllKeys()) {
            	respondAllKeys = true;
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
                    Settings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, scMode);
                    int ttl = settings.getResetTtlOnReadAtPercent();

                    attr.setRead(settings, scMode);

                    if (attr.linearize) {
                    	linearize = true;
                    }

                    if (spec.getProjectedBins() != null && spec.getProjectedBins().length > 0) {
                        // Read specific bins
                        rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, spec.getProjectedBins());
                    }
                    else {
                        // Read all bins
                        rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, true);
                    }
                }
                else if (spec.getOpType() == OpType.EXISTS) {
                    Settings settings = behavior.getSettings(OpKind.READ, OpShape.BATCH, scMode);
                    int ttl = settings.getResetTtlOnReadAtPercent();

                    attr.setRead(settings, scMode);

                    if (attr.linearize) {
                    	linearize = true;
                    }

                    rec = new BatchRead(key, spec.getWhereClause(), attr, ttl, false);
                }
                else {
                    Settings settings = behavior.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, scMode);

                    switch (spec.getOpType()) {
                    case UPSERT:
                    case UPDATE:
                    case INSERT:
                    case REPLACE:
                    case REPLACE_IF_EXISTS:
                    	attr.setWrite(settings, spec.getOpType());
                    	rec = new BatchWrite(key, attr, spec);
                        break;

                    case TOUCH:
                    	attr.setWrite(settings, spec.getOpType());
                        rec = new BatchWrite(key, attr, spec, List.of(Operation.touch()), OpType.TOUCH);
                        break;

                     case DELETE:
                    	attr.setDelete(settings);
                        rec = new BatchDelete(key, attr, spec);
                        break;

                    default:
                        throw new IllegalStateException("Unknown operation type: " + spec.getOpType());
                    }
                }

                if (scMode) {
                	mode = Mode.CP;
                }

                if (rec.hasWrite) {
					kind = OpKind.WRITE_NON_RETRYABLE;
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

		Settings settings = behavior.getSettings(kind, OpShape.BATCH, mode);

        BatchCommand parent = new BatchCommand(cluster, null, txn, null, records,
        	defaultWhereClause, respondAllKeys, linearize, settings);

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

	            // TODO Support user defined functions.
                case BATCH_UDF:
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
        }

        BatchExecutor.execute(cluster, commands, status);

        AsyncRecordStream recordStream = new AsyncRecordStream(records.size());

        try {
            for (int i = 0; i < records.size(); i++) {
                BatchRecord br = records.get(i);

                if (shouldIncludeResult(br.resultCode, respondAllKeys, failOnFilteredOut)) {
                    recordStream.publish(AbstractFilterableBuilder.createRecordResultFromBatchRecord(br, settings, i));
                }
            }
            return new RecordStream(recordStream);
        }
        finally {
            recordStream.complete();
        }
    }

    /**
     * Build RecordStream from batch results, respecting respondAllKeys and failOnFilteredOut flags.
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
                    if (settings.getStackTraceOnException() && br.resultCode != ResultCode.OK) {
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
        return shouldIncludeResult(resultCode, spec.isRespondAllKeys(), spec.isFailOnFilteredOut());
    }
*/
    /**
     * Determine if a result should be included based on result code and flags.
     */
    private static boolean shouldIncludeResult(int resultCode, boolean respondAllKeys, boolean failOnFilteredOut) {
        switch (resultCode) {
        case ResultCode.OK:
            return true;
        case ResultCode.KEY_NOT_FOUND_ERROR:
            return respondAllKeys;
        case ResultCode.FILTERED_OUT:
            return failOnFilteredOut || respondAllKeys;
        default:
            return true;  // Include errors in the stream
        }
    }
}
