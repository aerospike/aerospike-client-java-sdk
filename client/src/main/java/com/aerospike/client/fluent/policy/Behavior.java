package com.aerospike.client.fluent.policy;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Behavior {
    public static enum CommandType {
        ALL,
        READ_AP,
        READ_SC,
        WRITE_RETRYABLE,
        WRITE_NON_RETRYABLE,
        BATCH_READ,
        BATCH_WRITE,
        QUERY,
        INFO
    }

    public enum ExceptionPolicy {
        THROW_ON_ANY_ERROR,
        RETURN_AS_MANY_RESULTS_AS_POSSIBLE
    }

    private Map<CommandType, SettablePolicy> policies = new HashMap<>();

    public static final Behavior DEFAULT
            = new Behavior("default",
                    new BehaviorBuilder()
                        .forAllOperations()
                            .abandonCallAfter(Duration.ofSeconds(30))
                            .delayBetweenRetries(Duration.ofMillis(0))
                            .maximumNumberOfCallAttempts(1)
                            .replicaOrder(NodeCategory.SEQUENCE)
                            .resetTtlOnReadAtPercent(0)
                            .sendKey(true)
                            .useCompression(false)
                            .waitForCallToComplete(Duration.ofSeconds(1))
                            .waitForConnectionToComplete(Duration.ofSeconds(0))
                            .waitForSocketResponseAfterCallFails(Duration.ofSeconds(0))
                        .done()
                        .onAvailablityModeReads()
                            .migrationReadConsistency(ReadModeAP.ALL)
                        .done()
                        .onBatchReads()
                            .inline(BatchInline.INLINE_IN_MEMORY)
                        .done()
                        .onBatchWrites()
                        	.inline(BatchInline.INLINE_IN_MEMORY)
                        .done()
                        .onConsistencyModeReads()
                            .readConsistency(ReadModeSC.SESSION)
                        .done()
                        .onInfo()
                            .abandonCallAfter(Duration.ofSeconds(1))
                        .done()
                        .onNonRetryableWrites()
                            .useDurableDelete(false)
                        .done()
                        .onQuery()
                            .recordQueueSize(5000)
                            .maxConcurrentServers(0)
                            .maximumNumberOfCallAttempts(6)
                        .done()
                        .onRetryableWrites()
                            .useDurableDelete(false)
                            .maximumNumberOfCallAttempts(3)
                        .done()
                    ,
                    ExceptionPolicy.RETURN_AS_MANY_RESULTS_AS_POSSIBLE);


    public static interface BehaviorChanger {
        void change(BehaviorBuilder builder);
    }

    private final String name;
    private Behavior parent;
    private List<Behavior> children = new ArrayList<Behavior>();
    private ExceptionPolicy exceptionPolicy = null;
    private Boolean sendKey = null;
    private Boolean useCompression = null;
    private ConcurrentHashMap<CommandType, Object> cachedPolicies = new ConcurrentHashMap<>();

    private Behavior(String name) {
        this.name = name;
    }

    // Package level visibility
    Behavior(String name, BehaviorBuilder builder, ExceptionPolicy exceptionPolicy) {
        this.policies = builder.getPolicies();
        this.exceptionPolicy = exceptionPolicy;
        this.name = name;
    }

    /**
     * Invoke this method whenever the behavior is changed after construction. It will reform
     * its values from its parent and then notify children of the change.
     */
    void changed() {
        cachedPolicies.clear();
        for (Behavior child : children) {
            child.changed();
        }
    }
    public Behavior exceptionPolicy(ExceptionPolicy exceptionPolicy) {
        this.exceptionPolicy = exceptionPolicy;
        return this;
    }

    public String getName() {
        return name;
    }

    public Behavior getParent() {
        return parent;
    }

    /**
     * Get all children of this behavior
     *
     * @return List of child behaviors
     */
    public List<Behavior> getChildren() {
        return new ArrayList<>(children);
    }

    /**
     * Find a behavior by name in the tree starting from this behavior
     *
     * @param name The name of the behavior to find
     * @return Optional containing the behavior if found, or empty if not found
     */
    public Optional<Behavior> findBehavior(String name) {
        return BehaviorRegistry.getInstance().findInTree(this, name);
    }

    /**
     * Get a behavior by name from the registry
     *
     * @param name The name of the behavior to get
     * @return The behavior, or DEFAULT if not found
     */
    public static Behavior getBehavior(String name) {
        return BehaviorRegistry.getInstance().getBehaviorOrDefault(name);
    }


    public static Set<Behavior> getAllBehaviors() {
        return BehaviorRegistry.getInstance().getAllBehaviors().entrySet().stream()
            .map(entry -> entry.getValue())
            .collect(Collectors.toSet());
    }

    /**
     * Start monitoring a YAML file for behavior changes
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @throws IOException if there's an error setting up the file monitoring
     */
    public static void startMonitoring(String yamlFilePath) throws IOException {
        BehaviorFileMonitor.getInstance().startMonitoring(yamlFilePath);
    }

    /**
     * Start monitoring a YAML file for behavior changes and return a Closeable for use with try-with-resources
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @return Closeable instance that can be used with try-with-resources
     * @throws IOException if there's an error setting up the file monitoring
     */
    public static Closeable startMonitoringWithResource(String yamlFilePath) throws IOException {
        BehaviorFileMonitor monitor = BehaviorFileMonitor.getInstance();
        monitor.startMonitoring(yamlFilePath);
        return monitor;
    }

    /**
     * Start monitoring a YAML file for behavior changes with a custom reload delay
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @param reloadDelayMs The delay in milliseconds before reloading after a change
     * @throws IOException if there's an error setting up the file monitoring
     */
    public static void startMonitoring(String yamlFilePath, long reloadDelayMs) throws IOException {
        BehaviorFileMonitor.getInstance().startMonitoring(yamlFilePath, reloadDelayMs);
    }

    /**
     * Start monitoring a YAML file for behavior changes with a custom reload delay and return a Closeable for use with try-with-resources
     *
     * @param yamlFilePath The path to the YAML file to monitor
     * @param reloadDelayMs The delay in milliseconds before reloading after a change
     * @return Closeable instance that can be used with try-with-resources
     * @throws IOException if there's an error setting up the file monitoring
     */
    public static Closeable startMonitoringWithResource(String yamlFilePath, long reloadDelayMs) throws IOException {
        BehaviorFileMonitor monitor = BehaviorFileMonitor.getInstance();
        monitor.startMonitoring(yamlFilePath, reloadDelayMs);
        return monitor;
    }

    /**
     * Stop monitoring the YAML file
     */
    public static void stopMonitoring() {
        BehaviorFileMonitor.getInstance().stopMonitoring();
    }

    /**
     * Check if monitoring is active
     *
     * @return true if monitoring is active
     */
    public static boolean isMonitoring() {
        return BehaviorFileMonitor.getInstance().isMonitoring();
    }

    /**
     * Manually reload behaviors from the monitored YAML file
     */
    public static void reloadBehaviors() {
        BehaviorFileMonitor.getInstance().reloadBehaviors();
    }

    /**
     * Shutdown the file monitor
     */
    public static void shutdownMonitor() {
        BehaviorFileMonitor.getInstance().shutdown();
    }

    /**
     * Clear the policy cache for this behavior and all its children.
     * This is useful when behaviors are reloaded from configuration files.
     */
    void clearCache() {
        changed();
    }

    /**
     * Create a new policy from the existing policy. This will inherit all the behaviour of the parent from
     * which it is derived, but can then override whichever aspects it likes.
     * <p/>
     * Once a policy has been created, it is immutatable <b>except</b> for changes made by reloads the policies
     * via dynamic config.
     * <p/>
     * For example:
     * <pre>
     * Behavior newBehavior = Behavior.DEFAULT.deriveWithChanges("writeTest", builder -> {
     *    builder
     *       .forAllOperations()
     *           .resetTtlOnReadAtPercent(67)
     *           .useCompression(true)
     *       .done()
     *       .onRetryableWrites()
     *           .delayBetweenRetries(Duration.ofMillis(25))
     *           .maximumNumberOfCallAttempts(7)
     *       .done();
     * });
     * </pre>
     *
     * This will create a new behavior with the specified values set.
     * @param newName - the name of this policy, as specifiable in the dynamic config file
     * @param changer - The behavior changer used form a new behavior.
     * @return The new behaviour
     */
    public Behavior deriveWithChanges(String newName, BehaviorChanger changer) {
        Behavior result = new Behavior(newName);
        result.parent = this;
        this.children.add(result);

        BehaviorBuilder builder = new BehaviorBuilder();
        changer.change(builder);

        // Apply the changes to this result
        Map<CommandType, SettablePolicy> changedPolicies = builder.getPolicies();
        result.policies = changedPolicies;

        // Register the manually created behavior
        BehaviorRegistry.getInstance().registerBehavior(result);

        return result;
    }

    public ExceptionPolicy getExceptionPolicy() {
        return exceptionPolicy != null ? exceptionPolicy : parent != null ? parent.getExceptionPolicy() : ExceptionPolicy.RETURN_AS_MANY_RESULTS_AS_POSSIBLE;
    }

    public boolean getSendKey() {
        return (this.sendKey != null) ? this.sendKey : parent != null ? parent.getSendKey(): false;
    }

    public boolean getUseCompression() {
        return (this.useCompression != null) ? this.useCompression : parent != null ? parent.getUseCompression() : false;
    }

    public Behavior withUseCompression(boolean compress) {
        this.useCompression = compress;
        return this;
    }

    public Behavior withSendKey(boolean sendKey) {
        this.sendKey = sendKey;
        return this;
    }

    protected Behavior setPolicy(CommandType type, SettablePolicy policy) {
        return this;
    }

    @SuppressWarnings("unchecked")
    private <T extends SettablePolicy> T aggregateSettablePolicy(T aggregate, CommandType type) {
        T thisPolicy = (T)policies.get(type);
        aggregate.mergeFrom(thisPolicy);

        SettablePolicy theAllPolicy = policies.get(CommandType.ALL);
        aggregate.mergeFrom(theAllPolicy);

        if (parent != null) {
            parent.aggregateSettablePolicy(aggregate, type);
        }
        return aggregate;
    }

    public InfoPolicy getSharedInfoPolicy() {
        SettableInfoPolicy pol = aggregateSettablePolicy(new SettableInfoPolicy(), CommandType.INFO);
        return pol.formPolicy(new InfoPolicy());
    }

    public InfoPolicy getMutableInfoPolicy() {
        return new InfoPolicy(getSharedInfoPolicy());
    }

    public <T extends Policy> T getMutablePolicy(CommandType type) {
    	return getSharedPolicy(type);
    }

    /**
     * Get the policy of the appropriate type from this behavior. This will aggregate all the information on this
     * behavior (the specific requested type, plus the "All" type), ascending up the superclass behaviour tree
     * until it reaches the root level, then the policy is formed.
     * <p/>
     * The results are cached, so this traversal will only occur on the first time the policy is used after the
     * behavior is created, or when the dynamic config changes occur.
     * <p/>
     * <strong>Note:</strong>: Since {@code InfoPolicy} does not extend {@code Policy}, this method cannot be used
     * for {@code InfoPolicy}. Use {@link getSharedInfoPolicy()} instead
     * @param <T extends Policy>
     * @param type
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends Policy> T getSharedPolicy(CommandType type) {
        return (T)cachedPolicies.computeIfAbsent(type, cmdType -> {
            switch (type) {
            case WRITE_NON_RETRYABLE:
            case WRITE_RETRYABLE:
                return aggregateSettablePolicy(new SettableWritePolicy(), type)
                        .formPolicy();

            case BATCH_READ:
            case BATCH_WRITE:
                return aggregateSettablePolicy(new SettableBatchPolicy(), type)
                        .formPolicy();
            case QUERY:
                return aggregateSettablePolicy(new SettableQueryPolicy(), type)
                        .formPolicy();
            case READ_AP:
                return aggregateSettablePolicy(new SettableAvailabilityModeReadPolicy(), type)
                        .formPolicy();
            case READ_SC:
                return aggregateSettablePolicy(new SettableConsistencyModeReadPolicy(), type)
                        .formPolicy();
            case INFO:
                throw new IllegalArgumentException("Cannot pass 'INFO' to getSharedPolicy, use getSharedInfoPolicy instead");
            case ALL:
            default:
                throw new IllegalArgumentException("Cannot pass '" + type + "' to getSharedPolicy");
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T extends SettablePolicy> T getSettablePolicy(CommandType type) {
        return (T)cachedPolicies.computeIfAbsent(type, cmdType -> {
            switch (type) {
            case WRITE_NON_RETRYABLE:
            case WRITE_RETRYABLE:
                return aggregateSettablePolicy(new SettableWritePolicy(), type);

            case BATCH_READ:
            case BATCH_WRITE:
                return aggregateSettablePolicy(new SettableBatchPolicy(), type);

            case QUERY:
                return aggregateSettablePolicy(new SettableQueryPolicy(), type);

            case READ_AP:
                return aggregateSettablePolicy(new SettableAvailabilityModeReadPolicy(), type);

            case READ_SC:
                return aggregateSettablePolicy(new SettableConsistencyModeReadPolicy(), type);

            case INFO:
                throw new IllegalArgumentException("Cannot pass 'INFO' to getSharedPolicy, use getSharedInfoPolicy instead");

            case ALL:
            default:
                throw new IllegalArgumentException("Cannot pass '" + type + "' to getSharedPolicy");
            }
        });
    }

    public static void main(String[] args) throws Exception {
        Behavior beh = Behavior.DEFAULT.deriveWithChanges("writeTest", builder -> {
            builder
                .forAllOperations()
                    .resetTtlOnReadAtPercent(67)
                    .useCompression(true)
                .done()
                .onRetryableWrites()
                    .delayBetweenRetries(Duration.ofMillis(37))
                    .maximumNumberOfCallAttempts(7)
                .done()
            ;
        });

        Behavior beh2 = beh.deriveWithChanges("writeChild", builder -> {
            builder
                .onRetryableWrites()
                    .delayBetweenRetries(Duration.ofMillis(23))
                .done();
        });
        Behavior beh3 = beh.deriveWithChanges("writeChild2", builder -> {
            builder
                .forAllOperations()
                    .delayBetweenRetries(Duration.ofMillis(13))
                    .abandonCallAfter(Duration.ofSeconds(5))
                .done()
                .onInfo()
                    .abandonCallAfter(Duration.ofSeconds(3))
                .done();
        });

        for (Behavior behaviour : Behavior.getAllBehaviors()) {
            System.out.println(behaviour.getName());
        }
        Behavior.startMonitoring("/Users/tfaulkes/Programming/Aerospike/git/new-client-api/src/main/resources/behavior-example.yml");
        WritePolicy pol = Behavior.DEFAULT.getSharedPolicy(CommandType.WRITE_RETRYABLE);
        WritePolicy pol1 = beh.getSharedPolicy(CommandType.WRITE_RETRYABLE);
        WritePolicy pol2 = beh2.getSharedPolicy(CommandType.WRITE_RETRYABLE);
        WritePolicy pol3 = beh3.getSharedPolicy(CommandType.WRITE_RETRYABLE);

        System.out.println(pol.sleepBetweenRetries);
        System.out.println(pol1.sleepBetweenRetries);
        System.out.println(pol2.sleepBetweenRetries);
        System.out.println(pol3.sleepBetweenRetries);
        System.out.println(pol2.compress);
        System.out.println(pol2.maxRetries);
        System.out.println(pol2.readTouchTtlPercent);

        InfoPolicy infoPol = beh3.getSharedInfoPolicy();
        System.out.println(infoPol.timeout);

        for (Behavior behaviour : Behavior.getAllBehaviors()) {
            System.out.println(behaviour.getName());
        }

        Behavior highPerformance = Behavior.getBehavior("high-performance");
        System.out.println(highPerformance.getName());
        WritePolicy wp = highPerformance.getMutablePolicy(CommandType.WRITE_NON_RETRYABLE);
        System.out.println(wp.socketTimeout);
    }
}
