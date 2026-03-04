package com.aerospike.client.fluent;

/**
 * Intermediate builder for background UDF operations that requires specifying
 * the UDF function before any other configuration.
 *
 * <p>This builder enforces that the UDF package and function name must be specified
 * before the user can set arguments, apply filters, or execute the operation.
 * This provides compile-time safety against executing a background UDF without
 * specifying which function to run.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * ExecuteTask task = session.backgroundTask()
 *     .executeUdf(products)
 *     .function("myPackage", "applyDiscount")  // Required before execute()
 *     .passing(0.20)                           // Now available
 *     .where("$.stock > 250")                  // Now available
 *     .execute();
 * }</pre>
 *
 * @see BackgroundUdfBuilder
 * @see BackgroundTaskSession#executeUdf(DataSet)
 */
public class BackgroundUdfFunctionBuilder {

    private final Session session;
    private final DataSet dataset;

    /**
     * Package-private constructor.
     *
     * @param session the session to use for execution
     * @param dataset the dataset (namespace + set) to operate on
     */
    BackgroundUdfFunctionBuilder(Session session, DataSet dataset) {
        this.session = session;
        this.dataset = dataset;
    }

    /**
     * Specify the UDF package and function name to execute against matching records.
     *
     * <p>This method must be called to proceed with the background UDF operation.
     * After calling this method, you can optionally specify arguments with
     * {@link BackgroundUdfBuilder#passing(Object...)}, apply a filter with
     * {@link BackgroundUdfBuilder#where(String, Object...)}, or execute immediately.</p>
     *
     * @param packageName the name of the UDF package (registered on the server)
     * @param functionName the name of the function within the package
     * @return BackgroundUdfBuilder for further configuration and execution
     * @throws IllegalArgumentException if packageName or functionName is null or empty
     */
    public BackgroundUdfBuilder function(String packageName, String functionName) {
        if (packageName == null || packageName.isEmpty()) {
            throw new IllegalArgumentException("UDF package name cannot be null or empty");
        }
        if (functionName == null || functionName.isEmpty()) {
            throw new IllegalArgumentException("UDF function name cannot be null or empty");
        }

        return new BackgroundUdfBuilder(session, dataset, packageName, functionName);
    }
}
