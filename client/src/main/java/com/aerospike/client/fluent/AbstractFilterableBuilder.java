package com.aerospike.client.fluent;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.Settings;
import com.aerospike.client.fluent.query.WhereClauseProcessor;
import com.aerospike.dsl.ParseResult;

/**
 * Abstract base class for builders that support filtering operations.
 * Provides common functionality for where clauses, result filtering, and exception handling.
 */
public abstract class AbstractFilterableBuilder {
    protected WhereClauseProcessor dsl = null;
    protected boolean respondAllKeys = false;
    protected boolean failOnFilteredOut = false;
    
    /**
     * Set where clause, ensuring only one is specified.
     */
    protected void setWhereClause(WhereClauseProcessor clause) {
        if (this.dsl == null) {
            this.dsl = clause;
        } else {
            throw new IllegalArgumentException(
                "Only one 'where' clause can be specified. " +
                "There is already one of '%s' and another is being set to '%s'"
                .formatted(this.dsl, clause));
        }
    }
    
    /**
     * Process where clause and return Expression, or null if no clause exists.
     */
    protected Expression processWhereClause(String namespace, Session session) {
        if (this.dsl == null) {
            return null;
        }
        ParseResult parseResult = this.dsl.process(namespace, session);
        return Exp.build(parseResult.getExp());
    }
    
    /**
     * Determine if a result should be included based on result code and flags.
     */
    public boolean shouldIncludeResult(int resultCode) {
        return switch (resultCode) {
            case ResultCode.KEY_NOT_FOUND_ERROR -> respondAllKeys;
            case ResultCode.FILTERED_OUT -> failOnFilteredOut || respondAllKeys;
            default -> true;
        };
    }
    
    /**
     * Determine if an exception result should be published.
     */
    protected boolean shouldPublishException(AerospikeException ae) {
        return switch (ae.getResultCode()) {
            case ResultCode.FILTERED_OUT -> failOnFilteredOut || respondAllKeys;
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
        if (settings.getStackTraceOnException() && br.resultCode != ResultCode.OK) {
            return new RecordResult(
                br, 
                AerospikeException.resultCodeToException(br.resultCode, null, br.inDoubt), 
                index);
        } else {
            return new RecordResult(br, index);
        }
    }
    
    /**
     * Create WhereClauseProcessor from DSL string.
     */
    protected WhereClauseProcessor createWhereClauseProcessor(boolean allowSecondaryIndex, String dsl, Object... params) {
        if (dsl == null || dsl.isEmpty()) {
            return null;
        } else if (params.length == 0) {
            return WhereClauseProcessor.from(allowSecondaryIndex, dsl);
        } else {
            return WhereClauseProcessor.from(allowSecondaryIndex, String.format(dsl, params));
        }
    }
}

