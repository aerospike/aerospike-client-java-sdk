package com.aerospike.client.fluent;

import java.util.List;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.exp.Expression;

/**
 * Intermediate builder for UDF operations that requires specifying the UDF function
 * before any other operations can be performed.
 *
 * <p>This builder enforces that the UDF package and function name must be specified
 * before the user can set arguments, apply filters, or execute the operation.
 * This provides compile-time safety against executing a UDF without specifying
 * which function to run.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * // The function() call is required before execute()
 * session.executeUdf(key)
 *     .function("myPackage", "myFunction")  // Returns ChainableUdfBuilder
 *     .passing("arg1", 42)                   // Now available
 *     .execute();                            // Now available
 * }</pre>
 *
 * @see ChainableUdfBuilder
 */
public class UdfFunctionBuilder {

    private final Session session;
    private final List<Key> keys;
    private final List<OperationSpec> existingSpecs;
    private final Expression defaultWhereClause;
    private final long defaultExpirationInSeconds;
    private final Txn txnToUse;

    /**
     * Package-private constructor for creating a new UDF function builder.
     *
     * @param session the session to use for execution
     * @param keys the keys to execute the UDF on
     * @param existingSpecs existing operation specs from a chain (may be empty)
     * @param defaultWhereClause default where clause for the chain
     * @param defaultExpirationInSeconds default expiration for the chain
     * @param txnToUse transaction to use (may be null)
     */
    UdfFunctionBuilder(Session session, List<Key> keys, List<OperationSpec> existingSpecs,
                       Expression defaultWhereClause, long defaultExpirationInSeconds, Txn txnToUse) {
        this.session = session;
        this.keys = keys;
        this.existingSpecs = existingSpecs;
        this.defaultWhereClause = defaultWhereClause;
        this.defaultExpirationInSeconds = defaultExpirationInSeconds;
        this.txnToUse = txnToUse;
    }

    /**
     * Specify the UDF package and function name to execute.
     *
     * <p>This method must be called to proceed with the UDF operation.
     * After calling this method, you can optionally specify arguments with
     * {@link ChainableUdfBuilder#passing(Object...)}, set filters, or execute.</p>
     *
     * @param packageName the name of the UDF package (registered on the server)
     * @param functionName the name of the function within the package
     * @return ChainableUdfBuilder for further configuration and execution
     * @throws IllegalArgumentException if packageName or functionName is null or empty
     */
    public ChainableUdfBuilder function(String packageName, String functionName) {
        if (packageName == null || packageName.isEmpty()) {
            throw new IllegalArgumentException("UDF package name cannot be null or empty");
        }
        if (functionName == null || functionName.isEmpty()) {
            throw new IllegalArgumentException("UDF function name cannot be null or empty");
        }

        ChainableUdfBuilder builder = new ChainableUdfBuilder(
                session, existingSpecs, defaultWhereClause, defaultExpirationInSeconds, txnToUse);
        return builder.initUdfWithFunction(keys, packageName, functionName);
    }
}
