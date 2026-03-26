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

import com.aerospike.ael.ParseResult;
import com.aerospike.client.sdk.command.BatchRecord;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Settings;
import com.aerospike.client.sdk.query.WhereClauseProcessor;

/**
 * Abstract base class for builders that support filtering operations.
 * Provides common functionality for where clauses, result filtering, and exception handling.
 */
public abstract class AbstractFilterableBuilder {
    protected WhereClauseProcessor ael = null;
    protected boolean includeMissingKeys = false;
    protected boolean failOnFilteredOut = false;
    
    /**
     * Set where clause, ensuring only one is specified.
     */
    protected void setWhereClause(WhereClauseProcessor clause) {
        if (this.ael == null) {
            this.ael = clause;
        } else {
            throw new IllegalArgumentException(
                "Only one 'where' clause can be specified. " +
                "There is already one of '%s' and another is being set to '%s'"
                .formatted(this.ael, clause));
        }
    }
    
    /**
     * Process where clause and return Expression, or null if no clause exists.
     */
    protected Expression processWhereClause(String namespace, Session session) {
        if (this.ael == null) {
            return null;
        }
        ParseResult parseResult = this.ael.process(namespace, session);
        return Exp.build(parseResult.getExp());
    }
    
    /**
     * Determine if a result should be included based on result code and flags.
     */
    public boolean shouldIncludeResult(int resultCode) {
        return switch (resultCode) {
            case ResultCode.KEY_NOT_FOUND_ERROR -> includeMissingKeys;
            case ResultCode.FILTERED_OUT -> failOnFilteredOut;
            default -> true;
        };
    }
    
    /**
     * Determine if an exception result should be published (single-key context).
     */
    protected boolean shouldPublishException(AerospikeException ae) {
        return switch (ae.getResultCode()) {
            case ResultCode.FILTERED_OUT -> failOnFilteredOut;
            default -> true;
        };
    }
    
    /**
     * Create RecordResult from BatchRecord with proper stack trace handling.
     */
    public static RecordResult createRecordResultFromBatchRecord(
            BatchRecord br, 
            Settings settings, 
            int index) {
        if (settings.getStackTraceOnException() && isActionableError(br.resultCode)) {
            return new RecordResult(
                br, 
                AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt), 
                index);
        } else {
            return new RecordResult(br, index);
        }
    }
    
    /**
     * Create WhereClauseProcessor from AEL string.
     */
    protected WhereClauseProcessor createWhereClauseProcessor(boolean allowSecondaryIndex, String ael, Object... params) {
        if (ael == null || ael.isEmpty()) {
            return null;
        } else if (params.length == 0) {
            return WhereClauseProcessor.from(allowSecondaryIndex, ael);
        } else {
            return WhereClauseProcessor.from(allowSecondaryIndex, String.format(ael, params));
        }
    }

    /**
     * Determine the default {@link ErrorDisposition} for a list of operation specs.
     * Single-key operations default to {@code THROW}; multi-key (batch) operations
     * default to {@code IN_STREAM} so that partial results are not lost.
     */
    static ErrorDisposition defaultDisposition(List<OperationSpec> specs) {
        if (specs.size() == 1 && specs.get(0).getKeys().size() == 1) {
            return ErrorDisposition.THROW;
        }
        return ErrorDisposition.IN_STREAM;
    }

    /**
     * Returns true if the result code represents an actionable error that should
     * be subject to error disposition (THROW / HANDLER). Codes like
     * {@link ResultCode#KEY_NOT_FOUND_ERROR} are informational and should always
     * flow through the stream regardless of disposition.
     */
    static boolean isActionableError(int resultCode) {
        return resultCode != ResultCode.OK;
    }

    /**
     * Dispatch an async result: if a handler is present and the result is an error,
     * route to the handler; otherwise publish to the stream.
     */
    static void dispatchResult(RecordResult result, AsyncRecordStream stream, ErrorHandler handler) {
        if (handler != null && isActionableError(result.resultCode())) {
            dispatchError(result, handler);
        } else {
            stream.publish(result);
        }
    }

    /**
     * Collect a result into a list, mirroring {@link #dispatchResult(RecordResult, AsyncRecordStream, ErrorHandler)}.
     */
    static void dispatchResult(RecordResult result, List<RecordResult> results, ErrorHandler handler) {
        if (handler != null && isActionableError(result.resultCode())) {
            dispatchError(result, handler);
        } else {
            results.add(result);
        }
    }

    /**
     * Synchronously filter a RecordStream, dispatching errors to the handler
     * and returning a new stream containing only successful results.
     */
    public static RecordStream filterStreamErrors(RecordStream source, ErrorHandler handler) {
        List<RecordResult> filtered = new ArrayList<>();
        source.forEach(result -> {
            if (isActionableError(result.resultCode())) {
                dispatchError(result, handler);
            } else {
                filtered.add(result);
            }
        });
        return new RecordStream(filtered, 0);
    }

    static void dispatchError(RecordResult result, ErrorHandler handler) {
        AerospikeException ex = result.exception() != null
            ? result.exception()
            : AerospikeException.resultCodeToException(result.resultCode(), result.message(), result.inDoubt());
        handler.handle(result.key(), result.index(), ex);
    }
}

