package com.aerospike.client.fluent.policy;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Aerospike Policy Behavior Builder — Typed Selectors (non-generic bases)
 *
 * <h2>Overview</h2>
 * Provides a fluent, selector-driven API for configuring Aerospike operation policies with:
 * <ul>
 *   <li>Global → scoped → most-specific overrides (cascading configuration)</li>
 *   <li>Compile-time safety via typed selectors: only valid knobs are exposed per operation type</li>
 *   <li>Deterministic precedence: later patches overwrite earlier ones (last-writer wins)</li>
 *   <li>Parent behavior inheritance for creating configuration hierarchies</li>
 * </ul>
 *
 * <h2>Basic Usage</h2>
 * <p><b>Note:</b> All behaviors must derive from an existing behavior. The root of all behavior
 * hierarchies is {@link #DEFAULT}, which provides sensible defaults for all operations.</p>
 * 
 * <h3>Creating a derived behavior:</h3>
 * <pre>{@code
 * // Derive from DEFAULT with changes
 * Behavior production = Behavior.DEFAULT.deriveWithChanges("production", builder -> builder
 *     .on(Selectors.all(), ops -> ops
 *         .abandonCallAfter(Duration.ofSeconds(5))
 *     )
 *     .on(Selectors.writes().retryable().point().cp(), ops -> ops
 *         .useDurableDelete(true)
 *     )
 * );
 * 
 * // Create hierarchy: DEFAULT -> production -> productionHighLoad
 * Behavior productionHighLoad = production.deriveWithChanges("productionHighLoad", builder -> builder
 *     .on(Selectors.reads().batch(), ops -> ops
 *         .maxConcurrentNodes(16)
 *     )
 * );
 *     
 * // Retrieve resolved settings for a specific operation
 * Settings settings = production.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
 * }</pre>
 *
 * <h2>Selector Patterns</h2>
 * <h3>Recommended Pattern (General → Specific):</h3>
 * Build selector chains from general to specific for best type safety:
 * <pre>{@code
 * // READS: kind → shape → mode
 * Selectors.reads().batch().ap()     // ✓ Exposes readMode(), maxConcurrentNodes(), etc.
 * Selectors.reads().query().cp()     // ✓ Exposes consistency(), recordQueueSize(), etc.
 * 
 * // WRITES: kind → retryability → shape → mode
 * Selectors.writes().retryable().point().ap()    // ✓ Exposes commitLevel()
 * Selectors.writes().nonRetryable().batch().cp() // ✓ Exposes useDurableDelete()
 * }</pre>
 *
 * <h3>Alternative Pattern (Works but loses type safety):</h3>
 * Mode can be specified earlier, but intermediate steps lose type-specific methods:
 * <pre>{@code
 * // Mode selected first - works at runtime, but loses compile-time type safety
 * Selectors.writes().ap().retryable().point()  // ⚠ Works, but intermediate types don't expose commitLevel()
 * 
 * // Recommended alternative: select mode last
 * Selectors.writes().retryable().point().ap()  // ✓ Full type safety throughout chain
 * }</pre>
 *
 * <h2>Configuration Hierarchy</h2>
 * Settings cascade from general to specific:
 * <ol>
 *   <li>Parent behavior settings (if using {@code defaultsFrom()})</li>
 *   <li>Global settings ({@code Selectors.all()})</li>
 *   <li>Kind-specific settings ({@code Selectors.reads()}, {@code Selectors.writes()})</li>
 *   <li>Shape-specific settings ({@code .batch()}, {@code .point()}, {@code .query()})</li>
 *   <li>Mode-specific settings ({@code .ap()}, {@code .cp()})</li>
 * </ol>
 * Later configurations override earlier ones (last-writer wins).
 *
 * <h2>Operation Types</h2>
 * <ul>
 *   <li><b>OpKind:</b> READ, WRITE_RETRYABLE, WRITE_NON_RETRYABLE</li>
 *   <li><b>OpShape:</b> POINT (single record), BATCH (multiple records), QUERY (scan with filter)</li>
 *   <li><b>Mode:</b> AP (availability priority), CP (consistency priority)</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * Behavior instances are immutable and thread-safe once built. The builder is not thread-safe.
 *
 * @see #builder(String)
 * @see #getSettings(OpKind, OpShape, Mode)
 * @see Selectors
 */
public final class Behavior {

    // -----------------------------------------------------------------------------------
    // Legacy CommandType for backward compatibility
    // -----------------------------------------------------------------------------------
//    public static enum CommandType {
//        ALL,
//        READ_AP,
//        READ_SC,
//        WRITE_RETRYABLE,
//        WRITE_NON_RETRYABLE,
//        BATCH_READ,
//        BATCH_WRITE,
//        QUERY,
//        INFO
//    }


    // -----------------------------------------------------------------------------------
    // Internal factory (package-private for DEFAULT initialization only)
    // -----------------------------------------------------------------------------------
    private static BehaviorBuilder builder(String name) { return new BehaviorBuilderImpl(name, null); }

    public static final Behavior DEFAULT = Behavior.builder("DEFAULT")
            // Global defaults for all operations
            .on(Selectors.all(), ops -> ops
                    .abandonCallAfter(Duration.ofSeconds(1))
                    .delayBetweenRetries(Duration.ofMillis(0))
                    .maximumNumberOfCallAttempts(3)
                    .replicaOrder(Replica.SEQUENCE)  // Old: SEQUENCE, now explicit list
                    .readMode(ReadModeAP.ALL)
                    .consistency(ReadModeSC.SESSION)
                    .sendKey(true)
                    .useCompression(false)
                    .waitForCallToComplete(Duration.ofSeconds(30))
                    .waitForConnectionToComplete(Duration.ofSeconds(0))
                    .waitForSocketResponseAfterCallFails(Duration.ofSeconds(0))
            )
            .on(Selectors.reads(), ops -> ops
                    .resetTtlOnReadAtPercent(0)
            )
            // Batch read defaults
            .on(Selectors.reads().batch(), ops -> ops
                    .maxConcurrentNodes(1)  // Old: maxConcurrentServers
                    .allowInlineMemoryAccess(true)
                    .allowInlineSsdAccess(false)
            )
            // Query defaults
            .on(Selectors.reads().query(), ops -> ops
                    .recordQueueSize(5000)
                    // .maxConcurrentNodes(0)  // Queries don't have maxConcurrentNodes
                    .maximumNumberOfCallAttempts(6)
            )
            .on(Selectors.reads(), ops -> ops
                    .resetTtlOnReadAtPercent(0)
            )
            // Retryable write defaults
            .on(Selectors.writes().retryable(), ops -> ops
                    .useDurableDelete(false)
                    .maximumNumberOfCallAttempts(3)
                    .simulateXdrWrite(false)
            )
            .on(Selectors.writes().cp(), ops -> ops
                    .useDurableDelete(true)
            )
            // Non-retryable write defaults
            .on(Selectors.writes().nonRetryable(), ops -> ops
                    .maximumNumberOfCallAttempts(1)
                    .useDurableDelete(false)
                    .simulateXdrWrite(false)
            )
            // Batch write defaults (both retryable and non-retryable)
            .on(Selectors.writes().retryable().batch(), ops -> ops
                    .maxConcurrentNodes(1)
                    .allowInlineMemoryAccess(true)
                    .allowInlineSsdAccess(false)
            )
            .on(Selectors.writes().nonRetryable().batch(), ops -> ops
                    .maxConcurrentNodes(1)
                    .allowInlineMemoryAccess(true)
                    .allowInlineSsdAccess(false)
            )
            // AP write defaults
            .on(Selectors.writes().ap(), ops -> ops
                    .commitLevel(CommitLevel.COMMIT_ALL)
            )
            .build();

    // -----------------------------------------------------------------------------------
    // Behavior representation (patch list + resolved matrix)
    // -----------------------------------------------------------------------------------
    private final String name;
    private final List<Patch> patches; // in call order
    private final Behavior base;       // defaults (may be null)
    private final List<Behavior> children;
    private Map<OpKey, Settings> resolved; // fully-resolved matrix
//    private ConcurrentHashMap<CommandType, Object> legacyCachedPolicies = new ConcurrentHashMap<>();

    private Behavior(String name, List<Patch> patches, Behavior base) {
        this.name = name;
        this.patches = List.copyOf(patches);
        this.base = base;
        this.resolved = formMatrix();
        this.children = new ArrayList<>();
        
        if (base != null) {
            base.children.add(this);
        }
    }

    public void clearCache() {
        this.resolved = formMatrix();
//        this.legacyCachedPolicies.clear();
        // Notify all children
        for (Behavior child : children) {
            child.clearCache();
        }
    }
    
    /**
     * Invoke this method whenever the behavior is changed after construction. It will reform
     * its values from its parent and then notify children of the change.
     */
    void changed() {
        clearCache();
    }
    
    public String getName() {
        return name;
    }
    
    public Behavior getParent() {
        return this.base;
    }
    
    public List<Behavior> getChildren() {
        return Collections.unmodifiableList(children);
    }
    
    private Map<OpKey, Settings> formMatrix() {
        // 1) Start with parent's resolved matrix (if any).
        Map<OpKey, Settings> matrix = new HashMap<>();
        if (base != null && base.resolved != null) {
            // deep copy of settings
            for (Map.Entry<OpKey, Settings> e : base.resolved.entrySet()) {
                matrix.put(e.getKey(), copyOf(e.getValue()));
            }
        }

        // 2) Apply this behavior's patches in insertion order.
        //    For each concrete key matched by a patch, overwrite only non-null fields.
        List<OpKey> allKeys = listAllKeys();
        for (Patch p : patches) {
            for (OpKey key : allKeys) {
                if (applies(p.spec, key)) {
                    Settings acc = matrix.get(key);
                    if (acc == null) acc = new Settings();
                    mergeInto(acc, p.settings);
                    matrix.put(key, acc);
                }
            }
        }
        return matrix;
    }

    public String name() { return name; }

    /**
     * Get the resolved settings for a specific operation.
     * Returns null if no settings have been configured for this operation.
     * The settings are fully resolved, including inheritance from parent behaviors.
     */
    public Settings getSettings(OpKind kind, OpShape shape, Mode mode) {
        return resolved.get(new OpKey(kind, shape, mode));
    }

    /**
     * Get the resolved settings for a specific operation.
     * Returns null if no settings have been configured for this operation.
     * The settings are fully resolved, including inheritance from parent behaviors.
     */
    public Settings getSettings(OpKind kind, OpShape shape, boolean isNamespaceSC) {
        return resolved.get(new OpKey(kind, shape, isNamespaceSC ? Mode.CP : Mode.AP));
    }

//    // -----------------------------------------------------------------------------------
//    // Backward compatibility methods for legacy CommandType access
//    // -----------------------------------------------------------------------------------
//    
//    /**
//     * Get InfoPolicy using old-style access (backward compatibility)
//     */
//    public InfoPolicy getSharedInfoPolicy() {
//        // Info policies don't have operation-specific settings in the new model
//        // Use global settings
//        Settings settings = resolved.get(new OpKey(OpKind.READ, OpShape.POINT, Mode.AP));
//        if (settings == null) {
//            return new InfoPolicy();
//        }
//        InfoPolicy policy = new InfoPolicy();
//        if (settings.abandonCallAfter != null) {
//            policy.timeout = (int)settings.abandonCallAfter.toMillis();
//        }
//        return policy;
//    }

    /**
     * Get InfoPolicy (mutable copy) using old-style access (backward compatibility)
     */
//    public InfoPolicy getMutableInfoPolicy() {
//        return new InfoPolicy(getSharedInfoPolicy());
//    }

    /**
     * Get policy using legacy CommandType (backward compatibility)
     * 
     * @param type The command type
     * @return The policy
     */
//    @SuppressWarnings("unchecked")
//    public <T extends Policy> T getSharedPolicy(CommandType type) {
//        return (T)legacyCachedPolicies.computeIfAbsent(type, cmdType -> {
//            // Map CommandType to new dimensional system
//            switch (type) {
//            case WRITE_NON_RETRYABLE:
//                // Use settings for non-retryable point writes in AP mode (default)
//                Settings settings = resolved.get(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP));
//                return settings != null ? settings.asWritePolicy() : new WritePolicy();
//
//            case WRITE_RETRYABLE:
//                settings = resolved.get(new OpKey(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP));
//                return settings != null ? settings.asWritePolicy() : new WritePolicy();
//
//            case BATCH_READ:
//                settings = resolved.get(new OpKey(OpKind.READ, OpShape.BATCH, Mode.AP));
//                return settings != null ? settings.asBatchPolicy() : new BatchPolicy();
//                
//            case BATCH_WRITE:
//                settings = resolved.get(new OpKey(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP));
//                return settings != null ? settings.asBatchPolicy() : new BatchPolicy();
//                
//            case QUERY:
//                settings = resolved.get(new OpKey(OpKind.READ, OpShape.QUERY, Mode.AP));
//                return settings != null ? settings.asQueryPolicy() : new QueryPolicy();
//                
//            case READ_AP:
//                settings = resolved.get(new OpKey(OpKind.READ, OpShape.POINT, Mode.AP));
//                return settings != null ? settings.asReadPolicy() : new Policy();
//                
//            case READ_SC:
//                settings = resolved.get(new OpKey(OpKind.READ, OpShape.POINT, Mode.CP));
//                return settings != null ? settings.asReadPolicy() : new Policy();
//                
//            case INFO:
//                throw new IllegalArgumentException("Cannot pass 'INFO' to getSharedPolicy, use getSharedInfoPolicy instead");
//            case ALL:
//            default:
//                throw new IllegalArgumentException("Cannot pass '" + type + "' to getSharedPolicy");
//            }
//        });
//    }

    /**
     * Get mutable policy using legacy CommandType (backward compatibility)
     * 
     * @param type The command type
     * @return A mutable copy of the policy
     */
//    public <T extends Policy> T getMutablePolicy(CommandType type) {
//        return getSharedPolicy(type);
//    }

    /**
     * Creates a new Behavior derived from this one with additional changes.
     * This is a convenience method that automatically sets this behavior as the parent.
     * 
     * <h3>Usage:</h3>
     * <pre>{@code
     * Behavior child = parent.deriveWithChanges("childName", builder -> builder
     *     .on(Selectors.reads().batch().ap(), ops -> ops
     *         .maxConcurrentNodes(8)
     *     )
     *     .on(Selectors.writes().cp(), ops -> ops
     *         .useDurableDelete(true)
     *     )
     * );
     * }</pre>
     * 
     * @param name the name for the derived behavior
     * @param configurator a consumer that configures additional settings on the builder
     * @return a new Behavior with settings inherited from this one plus the configured changes
     */
    public Behavior deriveWithChanges(String name, java.util.function.Consumer<BehaviorBuilder> configurator) {
        BehaviorBuilder builder = new BehaviorBuilderImpl(name, this);
        configurator.accept(builder);
        Behavior newBehavior = builder.build();
        
        // Register the manually created behavior
        BehaviorRegistry.getInstance().registerBehavior(newBehavior);
        
        return newBehavior;
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

    /**
     * Get all registered behaviors
     *
     * @return Set of all behaviors
     */
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

    /** Debug helper: prints patches (in call order) and the resolved matrix. */
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("Behavior: ").append(name).append('\n');

        sb.append("--- Patches ---").append('\n');
        if (patches.isEmpty()) sb.append("(no overrides)").append('\n');
        int i = 0;
        for (Patch p : patches) {
            sb.append(String.format(Locale.ROOT, "%02d %s -> %s", ++i, p.spec, p.settings)).append('\n');
        }

        sb.append("--- Resolved Matrix ---").append('\n');
        for (OpKey k : listAllKeys()) {
            Settings s = resolved.get(k);
            if (s != null) sb.append(k).append(" => ").append(s).append('\n');
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------------------
    /**
     * Builder for constructing Behavior instances with fluent configuration.
     * 
     * <p><b>Note:</b> This builder is not directly accessible. Use {@link #deriveWithChanges(String, java.util.function.Consumer)}
     * to create new behaviors that inherit from an existing behavior.</p>
     * 
     * <h2>Basic Usage</h2>
     * 
     * <h3>Creating a derived Behavior:</h3>
     * <pre>{@code
     * // Derive from DEFAULT
     * Behavior production = Behavior.DEFAULT.deriveWithChanges("production", builder -> builder
     *     .on(Selectors.all(), ops -> ops
     *         .abandonCallAfter(Duration.ofSeconds(10))
     *         .maximumNumberOfCallAttempts(3)
     *     )
     *     .on(Selectors.reads().batch().ap(), ops -> ops
     *         .maxConcurrentNodes(8)
     *         .readMode(ReadModeAP.ALL)
     *     )
     * );
     * 
     * // Create a child behavior
     * Behavior productionHighLoad = production.deriveWithChanges("productionHighLoad", builder -> builder
     *     .on(Selectors.writes().cp(), ops -> ops
     *         .useDurableDelete(true)
     *     )
     * );
     * }</pre>
     * 
     * <h2>Configuration Patterns</h2>
     * 
     * <h3>Lambda Style (Recommended):</h3>
     * <pre>{@code
     * builder.on(selector, ops -> ops.method1().method2())
     * }</pre>
     * Returns the builder for chaining.
     * 
     * <h3>Ad-hoc Style (Alternative):</h3>
     * <pre>{@code
     * builder.on(selector).method1().method2()
     * }</pre>
     * Returns the tweaks view directly. Useful for single operations.
     * 
     * @see Behavior#builder(String)
     * @see Behavior#deriveWithChanges(String, java.util.function.Consumer)
     * @see Selectors
     */
    public interface BehaviorBuilder {
        /**
         * Sets the parent behavior from which this behavior will inherit settings.
         * Settings from the parent are applied first, then overridden by settings configured in this builder.
         * 
         * <p><b>Note:</b> Consider using {@link Behavior#deriveWithChanges(String, java.util.function.Consumer)}
         * instead, which is more intuitive for creating derived behaviors.
         * 
         * @param base the parent behavior to inherit from
         * @return this builder for chaining
         */
        BehaviorBuilder defaultsFrom(Behavior base);

        /**
         * Ad-hoc configuration style: returns the tweaks view for direct method chaining.
         * 
         * <p><b>Usage:</b>
         * <pre>{@code
         * builder.on(Selectors.reads().batch().ap())
         *     .maxConcurrentNodes(8)
         *     .readMode(ReadModeAP.ALL);
         * }</pre>
         * 
         * <p><b>Note:</b> The lambda style {@link #on(Selector, java.util.function.Consumer)} is preferred
         * for better readability with multiple configurations.
         * 
         * @param selector the selector specifying which operations to configure
         * @param <T> the type of tweaks view returned
         * @return the tweaks view for configuring settings
         */
        <T extends TweaksView> T on(Selector<T> selector);

        /**
         * Lambda configuration style (recommended): accepts a consumer to configure settings.
         * 
         * <p><b>Usage:</b>
         * <pre>{@code
         * builder.on(Selectors.reads().batch().ap(), ops -> ops
         *     .maxConcurrentNodes(8)
         *     .readMode(ReadModeAP.ALL)
         * );
         * }</pre>
         * 
         * <p>This style is preferred because:
         * <ul>
         *   <li>More readable for multiple settings</li>
         *   <li>Returns builder for continued chaining</li>
         *   <li>IDE can better format the lambda body</li>
         * </ul>
         * 
         * @param selector the selector specifying which operations to configure
         * @param apply the consumer that configures settings
         * @param <T> the type of tweaks view
         * @return this builder for chaining
         */
        <T extends TweaksView> BehaviorBuilder on(Selector<T> selector, java.util.function.Consumer<T> apply);

        /**
         * Builds the final immutable Behavior instance with all configured settings.
         * 
         * @return the constructed Behavior
         */
        Behavior build();
    }

    private static final class BehaviorBuilderImpl implements BehaviorBuilder {
        private final String name;
        private final List<Patch> patches = new ArrayList<>();
        private Behavior base;

        BehaviorBuilderImpl(String name, Behavior parent) {
            this.name = Objects.requireNonNull(name);
            this.base = parent; // null only for DEFAULT initialization
        }

        @Override public BehaviorBuilder defaultsFrom(Behavior base) {
            this.base = Objects.requireNonNull(base);
            return this;
        }

        @Override public <T extends TweaksView> T on(Selector<T> selector) {
            Objects.requireNonNull(selector, "selector");
            Patch patch = new Patch(selector.spec());
            patches.add(patch);
            TweaksProxy proxy = new TweaksProxy(patch);
            @SuppressWarnings("unchecked") T typed = (T) proxy;
            return typed;
        }

        @Override public <T extends TweaksView> BehaviorBuilder on(Selector<T> selector, java.util.function.Consumer<T> apply) {
            T tweaks = on(selector);
            apply.accept(tweaks);
            return this;
        }

        @Override public Behavior build() {
            return new Behavior(name, patches, base);
        }
    }

    // -----------------------------------------------------------------------------------
    // Dimensions
    // -----------------------------------------------------------------------------------
    public enum OpKind { READ, WRITE_RETRYABLE, WRITE_NON_RETRYABLE }
    public enum OpShape { ANY, POINT, BATCH, QUERY }
    public enum Mode { ANY, AP, CP }

    // -----------------------------------------------------------------------------------
    // Selection spec + resolution helpers
    // -----------------------------------------------------------------------------------
    static final class SelectionSpec {
        final OpKind kind;   // null == ALL kinds
        final OpShape shape; // ANY/POINT/BATCH/QUERY
        final Mode mode;     // ANY/AP/CP

        SelectionSpec(OpKind kind, OpShape shape, Mode mode) {
            this.kind = kind; this.shape = shape; this.mode = mode;
        }
        SelectionSpec withKind(OpKind k)  { return new SelectionSpec(k, shape, mode); }
        SelectionSpec withShape(OpShape s){ return new SelectionSpec(kind, s, mode); }
        SelectionSpec withMode(Mode m)    { return new SelectionSpec(kind, shape, m); }

        @Override public String toString() {
            return "[" + (kind == null? "ALL" : kind) + ", " + shape + ", " + mode + "]";
        }
    }

    /** Resolution key (concrete kind, shape, mode). */
    static final class OpKey {
        final OpKind kind; final OpShape shape; final Mode mode;
        OpKey(OpKind k, OpShape s, Mode m) { this.kind = k; this.shape = s; this.mode = m; }
        @Override public boolean equals(Object o) {
            if (!(o instanceof OpKey)) return false;
            OpKey x = (OpKey)o; return kind==x.kind && shape==x.shape && mode==x.mode;
        }
        @Override public int hashCode() { return Objects.hash(kind, shape, mode); }
        @Override public String toString(){ return kind + ":" + shape + ":" + mode; }
    }

    static boolean applies(SelectionSpec s, OpKey k) {
        if (s.kind  != null        && s.kind  != k.kind ) return false;
        if (s.shape != OpShape.ANY && s.shape != k.shape) return false;
        if (s.mode  != Mode.ANY    && s.mode  != k.mode ) return false;
        return true;
    }

    static List<OpKey> listAllKeys() {
        List<OpKey> out = new ArrayList<>();
        // READS
        for (Mode m : Mode.values()) {
            out.add(new OpKey(OpKind.READ, OpShape.POINT, m));
            out.add(new OpKey(OpKind.READ, OpShape.BATCH, m));
            out.add(new OpKey(OpKind.READ, OpShape.QUERY, m));
        }
        // RETRYABLE WRITES
        for (Mode m : Mode.values()) {
            out.add(new OpKey(OpKind.WRITE_RETRYABLE, OpShape.POINT, m));
            out.add(new OpKey(OpKind.WRITE_RETRYABLE, OpShape.BATCH, m));
        }
        // NON-RETRYABLE WRITES
        for (Mode m : Mode.values()) {
            out.add(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, m));
            out.add(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, m));
        }
        return out;
    }

    static Settings copyOf(Settings s) {
        if (s == null) return null;
        Settings t = new Settings();
        mergeInto(t, s);
        return t;
    }
    static void mergeInto(Settings dst, Settings src) {
        if (src.abandonCallAfter != null) dst.abandonCallAfter = src.abandonCallAfter;
        if (src.delayBetweenRetries != null) dst.delayBetweenRetries = src.delayBetweenRetries;
        if (src.maximumNumberOfCallAttempts != null) dst.maximumNumberOfCallAttempts = src.maximumNumberOfCallAttempts;
        if (src.replicaOrder != null) dst.replicaOrder = src.replicaOrder;
        if (src.sendKey != null) dst.sendKey = src.sendKey;
        if (src.useCompression != null) dst.useCompression = src.useCompression;
        if (src.waitForCallToComplete != null) dst.waitForCallToComplete = src.waitForCallToComplete;
        if (src.waitForConnectionToComplete != null) dst.waitForConnectionToComplete = src.waitForConnectionToComplete;
        if (src.waitForSocketResponseAfterCallFails != null) dst.waitForSocketResponseAfterCallFails = src.waitForSocketResponseAfterCallFails;
        
        if (src.recordQueueSize != null) dst.recordQueueSize = src.recordQueueSize;
        
        if (src.maxConcurrentNodes != null) dst.maxConcurrentNodes = src.maxConcurrentNodes;
        if (src.allowInlineMemoryAccess != null) dst.allowInlineMemoryAccess = src.allowInlineMemoryAccess;
        if (src.allowInlineSsdAccess != null) dst.allowInlineSsdAccess = src.allowInlineSsdAccess;
        
        if (src.useDurableDelete != null) dst.useDurableDelete = src.useDurableDelete;
        if (src.simulateXdrWrite != null) dst.simulateXdrWrite = src.simulateXdrWrite;

        if (src.commitLevel != null) dst.commitLevel = src.commitLevel;
        
        if (src.readModeAP != null) dst.readModeAP = src.readModeAP;
        if (src.readModeSC != null) dst.readModeSC = src.readModeSC;
        if (src.resetTtlOnReadAtPercent != null) dst.resetTtlOnReadAtPercent = src.resetTtlOnReadAtPercent;
    }

    // -----------------------------------------------------------------------------------
    // Settings captured by each patch (extend with your SettablePolicy knobs)
    // -----------------------------------------------------------------------------------
    
    static final class Patch {
        final SelectionSpec spec;
        final Settings settings = new Settings();
        Patch(SelectionSpec spec) { this.spec = spec; }
    }

    // -----------------------------------------------------------------------------------
    // Tweak view model — NON-GENERIC base + concrete views with covariant returns
    // -----------------------------------------------------------------------------------
    public interface TweaksView {}

    // Base (non-generic) surfaces
    public interface CommonTweaks extends TweaksView {
        CommonTweaks abandonCallAfter(Duration d);
        CommonTweaks delayBetweenRetries(Duration d);
        CommonTweaks maximumNumberOfCallAttempts(int n);
        CommonTweaks replicaOrder(Replica r);
        CommonTweaks sendKey(boolean sendKey);
        CommonTweaks useCompression(boolean compress);
        CommonTweaks waitForCallToComplete(Duration d);
        CommonTweaks waitForConnectionToComplete(Duration d);
        CommonTweaks waitForSocketResponseAfterCallFails(Duration d);
    }
    public interface QueryTweaks extends CommonTweaks {
        QueryTweaks recordQueueSize(int n);
    }
    public interface BatchTweaks extends CommonTweaks {
        BatchTweaks maxConcurrentNodes(int n);
        BatchTweaks allowInlineMemoryAccess(boolean v);
        BatchTweaks allowInlineSsdAccess(boolean v);
    }
    public interface WriteTweaks extends CommonTweaks {
        WriteTweaks useDurableDelete(boolean b);
        WriteTweaks simulateXdrWrite(boolean b);
    }
    public interface WriteApTweaks extends WriteTweaks {
        WriteApTweaks commitLevel(CommitLevel level);
    }
    public interface ReadTweaks extends CommonTweaks {
        ReadTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadApTweaks extends ReadTweaks {
        ReadApTweaks readMode(ReadModeAP mode);
    }
    public interface ReadCpTweaks extends ReadTweaks {
        ReadCpTweaks consistency(ReadModeSC c);
    }
    public interface RetryableWriteTweaks extends WriteTweaks {}
    public interface NonRetryableWriteTweaks extends WriteTweaks {}

    // Concrete Any-Mode markers (covariant return redeclaration)
    public interface AllAnyModeTweaks extends CommonTweaks {
        // Common tweaks
        @Override AllAnyModeTweaks abandonCallAfter(Duration d);
        @Override AllAnyModeTweaks delayBetweenRetries(Duration d);
        @Override AllAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override AllAnyModeTweaks replicaOrder(Replica r);
        @Override AllAnyModeTweaks sendKey(boolean sendKey);
        @Override AllAnyModeTweaks useCompression(boolean compress);
        @Override AllAnyModeTweaks waitForCallToComplete(Duration d);
        @Override AllAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override AllAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        
        // Read-specific settings
        AllAnyModeTweaks resetTtlOnReadAtPercent(int percent);
        AllAnyModeTweaks readMode(ReadModeAP mode);
        AllAnyModeTweaks consistency(ReadModeSC c);
        
        // Write-specific settings
        AllAnyModeTweaks useDurableDelete(boolean b);
        AllAnyModeTweaks simulateXdrWrite(boolean b);
        AllAnyModeTweaks commitLevel(CommitLevel level);
        
        // Batch-specific settings
        AllAnyModeTweaks maxConcurrentNodes(int n);
        AllAnyModeTweaks allowInlineMemoryAccess(boolean v);
        AllAnyModeTweaks allowInlineSsdAccess(boolean v);
        
        // Query-specific settings
        AllAnyModeTweaks recordQueueSize(int n);
    }
    public interface ReadAnyAnyModeTweaks extends ReadTweaks {
        @Override ReadAnyAnyModeTweaks abandonCallAfter(Duration d);
        @Override ReadAnyAnyModeTweaks delayBetweenRetries(Duration d);
        @Override ReadAnyAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadAnyAnyModeTweaks replicaOrder(Replica r);
        @Override ReadAnyAnyModeTweaks sendKey(boolean sendKey);
        @Override ReadAnyAnyModeTweaks useCompression(boolean compress);
        @Override ReadAnyAnyModeTweaks waitForCallToComplete(Duration d);
        @Override ReadAnyAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override ReadAnyAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadAnyAnyModeTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadAnyApTweaks extends ReadApTweaks {
        @Override ReadAnyApTweaks abandonCallAfter(Duration d);
        @Override ReadAnyApTweaks delayBetweenRetries(Duration d);
        @Override ReadAnyApTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadAnyApTweaks replicaOrder(Replica r);
        @Override ReadAnyApTweaks sendKey(boolean sendKey);
        @Override ReadAnyApTweaks useCompression(boolean compress);
        @Override ReadAnyApTweaks waitForCallToComplete(Duration d);
        @Override ReadAnyApTweaks waitForConnectionToComplete(Duration d);
        @Override ReadAnyApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadAnyApTweaks readMode(ReadModeAP mode);
        @Override ReadAnyApTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadAnyCpTweaks extends ReadCpTweaks {
        @Override ReadAnyCpTweaks abandonCallAfter(Duration d);
        @Override ReadAnyCpTweaks delayBetweenRetries(Duration d);
        @Override ReadAnyCpTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadAnyCpTweaks replicaOrder(Replica r);
        @Override ReadAnyCpTweaks sendKey(boolean sendKey);
        @Override ReadAnyCpTweaks useCompression(boolean compress);
        @Override ReadAnyCpTweaks waitForCallToComplete(Duration d);
        @Override ReadAnyCpTweaks waitForConnectionToComplete(Duration d);
        @Override ReadAnyCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadAnyCpTweaks consistency(ReadModeSC c);
        @Override ReadAnyCpTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface WriteRootAnyModeTweaks extends WriteTweaks {
        @Override WriteRootAnyModeTweaks abandonCallAfter(Duration d);
        @Override WriteRootAnyModeTweaks delayBetweenRetries(Duration d);
        @Override WriteRootAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteRootAnyModeTweaks replicaOrder(Replica r);
        @Override WriteRootAnyModeTweaks sendKey(boolean sendKey);
        @Override WriteRootAnyModeTweaks useCompression(boolean compress);
        @Override WriteRootAnyModeTweaks waitForCallToComplete(Duration d);
        @Override WriteRootAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override WriteRootAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteRootAnyModeTweaks useDurableDelete(boolean b);
        @Override WriteRootAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface WriteRootApTweaks extends WriteApTweaks {
        @Override WriteRootApTweaks abandonCallAfter(Duration d);
        @Override WriteRootApTweaks delayBetweenRetries(Duration d);
        @Override WriteRootApTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteRootApTweaks replicaOrder(Replica r);
        @Override WriteRootApTweaks sendKey(boolean sendKey);
        @Override WriteRootApTweaks useCompression(boolean compress);
        @Override WriteRootApTweaks waitForCallToComplete(Duration d);
        @Override WriteRootApTweaks waitForConnectionToComplete(Duration d);
        @Override WriteRootApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteRootApTweaks useDurableDelete(boolean b);
        @Override WriteRootApTweaks simulateXdrWrite(boolean b);
        @Override WriteRootApTweaks commitLevel(CommitLevel level);
    }
    public interface WriteRootCpTweaks extends WriteTweaks {
        @Override WriteRootCpTweaks abandonCallAfter(Duration d);
        @Override WriteRootCpTweaks delayBetweenRetries(Duration d);
        @Override WriteRootCpTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteRootCpTweaks replicaOrder(Replica r);
        @Override WriteRootCpTweaks sendKey(boolean sendKey);
        @Override WriteRootCpTweaks useCompression(boolean compress);
        @Override WriteRootCpTweaks waitForCallToComplete(Duration d);
        @Override WriteRootCpTweaks waitForConnectionToComplete(Duration d);
        @Override WriteRootCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteRootCpTweaks useDurableDelete(boolean b);
        @Override WriteRootCpTweaks simulateXdrWrite(boolean b);
    }

    // READ concrete views
    public interface ReadPointAnyModeTweaks extends CommonTweaks {
        @Override ReadPointAnyModeTweaks abandonCallAfter(Duration d);
        @Override ReadPointAnyModeTweaks delayBetweenRetries(Duration d);
        @Override ReadPointAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadPointAnyModeTweaks replicaOrder(Replica r);
        @Override ReadPointAnyModeTweaks sendKey(boolean sendKey);
        @Override ReadPointAnyModeTweaks useCompression(boolean compress);
        @Override ReadPointAnyModeTweaks waitForCallToComplete(Duration d);
        @Override ReadPointAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override ReadPointAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
    }
    public interface ReadBatchAnyModeTweaks extends BatchTweaks {
        @Override ReadBatchAnyModeTweaks abandonCallAfter(Duration d);
        @Override ReadBatchAnyModeTweaks delayBetweenRetries(Duration d);
        @Override ReadBatchAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadBatchAnyModeTweaks replicaOrder(Replica r);
        @Override ReadBatchAnyModeTweaks sendKey(boolean sendKey);
        @Override ReadBatchAnyModeTweaks useCompression(boolean compress);
        @Override ReadBatchAnyModeTweaks waitForCallToComplete(Duration d);
        @Override ReadBatchAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override ReadBatchAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadBatchAnyModeTweaks maxConcurrentNodes(int n);
        @Override ReadBatchAnyModeTweaks allowInlineMemoryAccess(boolean v);
        @Override ReadBatchAnyModeTweaks allowInlineSsdAccess(boolean v);
    }
    public interface ReadQueryAnyModeTweaks extends QueryTweaks {
        @Override ReadQueryAnyModeTweaks abandonCallAfter(Duration d);
        @Override ReadQueryAnyModeTweaks delayBetweenRetries(Duration d);
        @Override ReadQueryAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadQueryAnyModeTweaks replicaOrder(Replica r);
        @Override ReadQueryAnyModeTweaks sendKey(boolean sendKey);
        @Override ReadQueryAnyModeTweaks useCompression(boolean compress);
        @Override ReadQueryAnyModeTweaks waitForCallToComplete(Duration d);
        @Override ReadQueryAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override ReadQueryAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadQueryAnyModeTweaks recordQueueSize(int n);
    }
    public interface ReadPointApTweaks extends ReadApTweaks {
        @Override ReadPointApTweaks abandonCallAfter(Duration d);
        @Override ReadPointApTweaks delayBetweenRetries(Duration d);
        @Override ReadPointApTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadPointApTweaks replicaOrder(Replica r);
        @Override ReadPointApTweaks sendKey(boolean sendKey);
        @Override ReadPointApTweaks useCompression(boolean compress);
        @Override ReadPointApTweaks waitForCallToComplete(Duration d);
        @Override ReadPointApTweaks waitForConnectionToComplete(Duration d);
        @Override ReadPointApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadPointApTweaks readMode(ReadModeAP mode);
        @Override ReadPointApTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadPointCpTweaks extends ReadCpTweaks {
        @Override ReadPointCpTweaks abandonCallAfter(Duration d);
        @Override ReadPointCpTweaks delayBetweenRetries(Duration d);
        @Override ReadPointCpTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadPointCpTweaks replicaOrder(Replica r);
        @Override ReadPointCpTweaks sendKey(boolean sendKey);
        @Override ReadPointCpTweaks useCompression(boolean compress);
        @Override ReadPointCpTweaks waitForCallToComplete(Duration d);
        @Override ReadPointCpTweaks waitForConnectionToComplete(Duration d);
        @Override ReadPointCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadPointCpTweaks consistency(ReadModeSC c);
        @Override ReadPointCpTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadBatchApTweaks extends BatchTweaks, ReadApTweaks {
        @Override ReadBatchApTweaks abandonCallAfter(Duration d);
        @Override ReadBatchApTweaks delayBetweenRetries(Duration d);
        @Override ReadBatchApTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadBatchApTweaks replicaOrder(Replica r);
        @Override ReadBatchApTweaks sendKey(boolean sendKey);
        @Override ReadBatchApTweaks useCompression(boolean compress);
        @Override ReadBatchApTweaks waitForCallToComplete(Duration d);
        @Override ReadBatchApTweaks waitForConnectionToComplete(Duration d);
        @Override ReadBatchApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadBatchApTweaks maxConcurrentNodes(int n);
        @Override ReadBatchApTweaks allowInlineMemoryAccess(boolean v);
        @Override ReadBatchApTweaks allowInlineSsdAccess(boolean v);
        @Override ReadBatchApTweaks readMode(ReadModeAP mode);
        @Override ReadBatchApTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadBatchCpTweaks extends BatchTweaks, ReadCpTweaks {
        @Override ReadBatchCpTweaks abandonCallAfter(Duration d);
        @Override ReadBatchCpTweaks delayBetweenRetries(Duration d);
        @Override ReadBatchCpTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadBatchCpTweaks replicaOrder(Replica r);
        @Override ReadBatchCpTweaks sendKey(boolean sendKey);
        @Override ReadBatchCpTweaks useCompression(boolean compress);
        @Override ReadBatchCpTweaks waitForCallToComplete(Duration d);
        @Override ReadBatchCpTweaks waitForConnectionToComplete(Duration d);
        @Override ReadBatchCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadBatchCpTweaks maxConcurrentNodes(int n);
        @Override ReadBatchCpTweaks allowInlineMemoryAccess(boolean v);
        @Override ReadBatchCpTweaks allowInlineSsdAccess(boolean v);
        @Override ReadBatchCpTweaks consistency(ReadModeSC c);
        @Override ReadBatchCpTweaks resetTtlOnReadAtPercent(int percent);
    }
    public interface ReadQueryApTweaks extends ReadApTweaks, QueryTweaks {
        @Override ReadQueryApTweaks abandonCallAfter(Duration d);
        @Override ReadQueryApTweaks delayBetweenRetries(Duration d);
        @Override ReadQueryApTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadQueryApTweaks replicaOrder(Replica r);
        @Override ReadQueryApTweaks sendKey(boolean sendKey);
        @Override ReadQueryApTweaks useCompression(boolean compress);
        @Override ReadQueryApTweaks waitForCallToComplete(Duration d);
        @Override ReadQueryApTweaks waitForConnectionToComplete(Duration d);
        @Override ReadQueryApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadQueryApTweaks readMode(ReadModeAP mode);
        @Override ReadQueryApTweaks resetTtlOnReadAtPercent(int percent);
        @Override ReadQueryApTweaks recordQueueSize(int n);
    }
    public interface ReadQueryCpTweaks extends ReadCpTweaks, QueryTweaks {
        @Override ReadQueryCpTweaks abandonCallAfter(Duration d);
        @Override ReadQueryCpTweaks delayBetweenRetries(Duration d);
        @Override ReadQueryCpTweaks maximumNumberOfCallAttempts(int n);
        @Override ReadQueryCpTweaks replicaOrder(Replica r);
        @Override ReadQueryCpTweaks sendKey(boolean sendKey);
        @Override ReadQueryCpTweaks useCompression(boolean compress);
        @Override ReadQueryCpTweaks waitForCallToComplete(Duration d);
        @Override ReadQueryCpTweaks waitForConnectionToComplete(Duration d);
        @Override ReadQueryCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override ReadQueryCpTweaks consistency(ReadModeSC c);
        @Override ReadQueryCpTweaks resetTtlOnReadAtPercent(int percent);
        @Override ReadQueryCpTweaks recordQueueSize(int n);
    }

    // WRITE shape concrete views (retryability-agnostic)
    public interface WritePointAnyModeTweaks extends WriteTweaks {
        @Override WritePointAnyModeTweaks abandonCallAfter(Duration d);
        @Override WritePointAnyModeTweaks delayBetweenRetries(Duration d);
        @Override WritePointAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override WritePointAnyModeTweaks replicaOrder(Replica r);
        @Override WritePointAnyModeTweaks sendKey(boolean sendKey);
        @Override WritePointAnyModeTweaks useCompression(boolean compress);
        @Override WritePointAnyModeTweaks waitForCallToComplete(Duration d);
        @Override WritePointAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override WritePointAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WritePointAnyModeTweaks useDurableDelete(boolean b);
        @Override WritePointAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface WritePointApTweaks extends WriteApTweaks {
        @Override WritePointApTweaks abandonCallAfter(Duration d);
        @Override WritePointApTweaks delayBetweenRetries(Duration d);
        @Override WritePointApTweaks maximumNumberOfCallAttempts(int n);
        @Override WritePointApTweaks replicaOrder(Replica r);
        @Override WritePointApTweaks sendKey(boolean sendKey);
        @Override WritePointApTweaks useCompression(boolean compress);
        @Override WritePointApTweaks waitForCallToComplete(Duration d);
        @Override WritePointApTweaks waitForConnectionToComplete(Duration d);
        @Override WritePointApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WritePointApTweaks useDurableDelete(boolean b);
        @Override WritePointApTweaks simulateXdrWrite(boolean b);
        @Override WritePointApTweaks commitLevel(CommitLevel level);
    }
    public interface WritePointCpTweaks extends WriteTweaks {
        @Override WritePointCpTweaks abandonCallAfter(Duration d);
        @Override WritePointCpTweaks delayBetweenRetries(Duration d);
        @Override WritePointCpTweaks maximumNumberOfCallAttempts(int n);
        @Override WritePointCpTweaks replicaOrder(Replica r);
        @Override WritePointCpTweaks sendKey(boolean sendKey);
        @Override WritePointCpTweaks useCompression(boolean compress);
        @Override WritePointCpTweaks waitForCallToComplete(Duration d);
        @Override WritePointCpTweaks waitForConnectionToComplete(Duration d);
        @Override WritePointCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WritePointCpTweaks useDurableDelete(boolean b);
        @Override WritePointCpTweaks simulateXdrWrite(boolean b);
    }
    public interface WriteBatchAnyModeTweaks extends BatchTweaks, WriteTweaks {
        @Override WriteBatchAnyModeTweaks abandonCallAfter(Duration d);
        @Override WriteBatchAnyModeTweaks delayBetweenRetries(Duration d);
        @Override WriteBatchAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteBatchAnyModeTweaks replicaOrder(Replica r);
        @Override WriteBatchAnyModeTweaks sendKey(boolean sendKey);
        @Override WriteBatchAnyModeTweaks useCompression(boolean compress);
        @Override WriteBatchAnyModeTweaks waitForCallToComplete(Duration d);
        @Override WriteBatchAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override WriteBatchAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteBatchAnyModeTweaks maxConcurrentNodes(int n);
        @Override WriteBatchAnyModeTweaks allowInlineMemoryAccess(boolean v);
        @Override WriteBatchAnyModeTweaks allowInlineSsdAccess(boolean v);
        @Override WriteBatchAnyModeTweaks useDurableDelete(boolean b);
        @Override WriteBatchAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface WriteBatchApTweaks extends BatchTweaks, WriteApTweaks {
        @Override WriteBatchApTweaks abandonCallAfter(Duration d);
        @Override WriteBatchApTweaks delayBetweenRetries(Duration d);
        @Override WriteBatchApTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteBatchApTweaks replicaOrder(Replica r);
        @Override WriteBatchApTweaks sendKey(boolean sendKey);
        @Override WriteBatchApTweaks useCompression(boolean compress);
        @Override WriteBatchApTweaks waitForCallToComplete(Duration d);
        @Override WriteBatchApTweaks waitForConnectionToComplete(Duration d);
        @Override WriteBatchApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteBatchApTweaks maxConcurrentNodes(int n);
        @Override WriteBatchApTweaks allowInlineMemoryAccess(boolean v);
        @Override WriteBatchApTweaks allowInlineSsdAccess(boolean v);
        @Override WriteBatchApTweaks useDurableDelete(boolean b);
        @Override WriteBatchApTweaks simulateXdrWrite(boolean b);
        @Override WriteBatchApTweaks commitLevel(CommitLevel level);
    }
    public interface WriteBatchCpTweaks extends BatchTweaks, WriteTweaks {
        @Override WriteBatchCpTweaks abandonCallAfter(Duration d);
        @Override WriteBatchCpTweaks delayBetweenRetries(Duration d);
        @Override WriteBatchCpTweaks maximumNumberOfCallAttempts(int n);
        @Override WriteBatchCpTweaks replicaOrder(Replica r);
        @Override WriteBatchCpTweaks sendKey(boolean sendKey);
        @Override WriteBatchCpTweaks useCompression(boolean compress);
        @Override WriteBatchCpTweaks waitForCallToComplete(Duration d);
        @Override WriteBatchCpTweaks waitForConnectionToComplete(Duration d);
        @Override WriteBatchCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override WriteBatchCpTweaks maxConcurrentNodes(int n);
        @Override WriteBatchCpTweaks allowInlineMemoryAccess(boolean v);
        @Override WriteBatchCpTweaks allowInlineSsdAccess(boolean v);
        @Override WriteBatchCpTweaks useDurableDelete(boolean b);
        @Override WriteBatchCpTweaks simulateXdrWrite(boolean b);
    }

    // WRITE concrete views
    public interface RetryableWriteAnyModeTweaks extends RetryableWriteTweaks {
        @Override RetryableWriteAnyModeTweaks abandonCallAfter(Duration d);
        @Override RetryableWriteAnyModeTweaks delayBetweenRetries(Duration d);
        @Override RetryableWriteAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWriteAnyModeTweaks replicaOrder(Replica r);
        @Override RetryableWriteAnyModeTweaks sendKey(boolean sendKey);
        @Override RetryableWriteAnyModeTweaks useCompression(boolean compress);
        @Override RetryableWriteAnyModeTweaks waitForCallToComplete(Duration d);
        @Override RetryableWriteAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWriteAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWriteAnyModeTweaks useDurableDelete(boolean b);
        @Override RetryableWriteAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface RetryableWritePointAnyModeTweaks extends RetryableWriteTweaks {
        @Override RetryableWritePointAnyModeTweaks abandonCallAfter(Duration d);
        @Override RetryableWritePointAnyModeTweaks delayBetweenRetries(Duration d);
        @Override RetryableWritePointAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWritePointAnyModeTweaks replicaOrder(Replica r);
        @Override RetryableWritePointAnyModeTweaks sendKey(boolean sendKey);
        @Override RetryableWritePointAnyModeTweaks useCompression(boolean compress);
        @Override RetryableWritePointAnyModeTweaks waitForCallToComplete(Duration d);
        @Override RetryableWritePointAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWritePointAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWritePointAnyModeTweaks useDurableDelete(boolean b);
        @Override RetryableWritePointAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface RetryableWritePointApTweaks extends WriteApTweaks {
        @Override RetryableWritePointApTweaks abandonCallAfter(Duration d);
        @Override RetryableWritePointApTweaks delayBetweenRetries(Duration d);
        @Override RetryableWritePointApTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWritePointApTweaks replicaOrder(Replica r);
        @Override RetryableWritePointApTweaks sendKey(boolean sendKey);
        @Override RetryableWritePointApTweaks useCompression(boolean compress);
        @Override RetryableWritePointApTweaks waitForCallToComplete(Duration d);
        @Override RetryableWritePointApTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWritePointApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWritePointApTweaks useDurableDelete(boolean b);
        @Override RetryableWritePointApTweaks simulateXdrWrite(boolean b);
        @Override RetryableWritePointApTweaks commitLevel(CommitLevel level);
    }
    public interface RetryableWritePointCpTweaks extends RetryableWriteTweaks {
        @Override RetryableWritePointCpTweaks abandonCallAfter(Duration d);
        @Override RetryableWritePointCpTweaks delayBetweenRetries(Duration d);
        @Override RetryableWritePointCpTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWritePointCpTweaks replicaOrder(Replica r);
        @Override RetryableWritePointCpTweaks sendKey(boolean sendKey);
        @Override RetryableWritePointCpTweaks useCompression(boolean compress);
        @Override RetryableWritePointCpTweaks waitForCallToComplete(Duration d);
        @Override RetryableWritePointCpTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWritePointCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWritePointCpTweaks useDurableDelete(boolean b);
        @Override RetryableWritePointCpTweaks simulateXdrWrite(boolean b);
    }
    public interface RetryableWriteBatchAnyModeTweaks extends BatchTweaks, RetryableWriteTweaks {
        @Override RetryableWriteBatchAnyModeTweaks abandonCallAfter(Duration d);
        @Override RetryableWriteBatchAnyModeTweaks delayBetweenRetries(Duration d);
        @Override RetryableWriteBatchAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWriteBatchAnyModeTweaks replicaOrder(Replica r);
        @Override RetryableWriteBatchAnyModeTweaks sendKey(boolean sendKey);
        @Override RetryableWriteBatchAnyModeTweaks useCompression(boolean compress);
        @Override RetryableWriteBatchAnyModeTweaks waitForCallToComplete(Duration d);
        @Override RetryableWriteBatchAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWriteBatchAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWriteBatchAnyModeTweaks maxConcurrentNodes(int n);
        @Override RetryableWriteBatchAnyModeTweaks allowInlineMemoryAccess(boolean v);
        @Override RetryableWriteBatchAnyModeTweaks allowInlineSsdAccess(boolean v);
        @Override RetryableWriteBatchAnyModeTweaks useDurableDelete(boolean b);
        @Override RetryableWriteBatchAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface RetryableWriteBatchApTweaks extends BatchTweaks, WriteApTweaks {
        @Override RetryableWriteBatchApTweaks abandonCallAfter(Duration d);
        @Override RetryableWriteBatchApTweaks delayBetweenRetries(Duration d);
        @Override RetryableWriteBatchApTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWriteBatchApTweaks replicaOrder(Replica r);
        @Override RetryableWriteBatchApTweaks sendKey(boolean sendKey);
        @Override RetryableWriteBatchApTweaks useCompression(boolean compress);
        @Override RetryableWriteBatchApTweaks waitForCallToComplete(Duration d);
        @Override RetryableWriteBatchApTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWriteBatchApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWriteBatchApTweaks maxConcurrentNodes(int n);
        @Override RetryableWriteBatchApTweaks allowInlineMemoryAccess(boolean v);
        @Override RetryableWriteBatchApTweaks allowInlineSsdAccess(boolean v);
        @Override RetryableWriteBatchApTweaks useDurableDelete(boolean b);
        @Override RetryableWriteBatchApTweaks simulateXdrWrite(boolean b);
        @Override RetryableWriteBatchApTweaks commitLevel(CommitLevel level);
    }
    public interface RetryableWriteBatchCpTweaks extends BatchTweaks, RetryableWriteTweaks {
        @Override RetryableWriteBatchCpTweaks abandonCallAfter(Duration d);
        @Override RetryableWriteBatchCpTweaks delayBetweenRetries(Duration d);
        @Override RetryableWriteBatchCpTweaks maximumNumberOfCallAttempts(int n);
        @Override RetryableWriteBatchCpTweaks replicaOrder(Replica r);
        @Override RetryableWriteBatchCpTweaks sendKey(boolean sendKey);
        @Override RetryableWriteBatchCpTweaks useCompression(boolean compress);
        @Override RetryableWriteBatchCpTweaks waitForCallToComplete(Duration d);
        @Override RetryableWriteBatchCpTweaks waitForConnectionToComplete(Duration d);
        @Override RetryableWriteBatchCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override RetryableWriteBatchCpTweaks maxConcurrentNodes(int n);
        @Override RetryableWriteBatchCpTweaks allowInlineMemoryAccess(boolean v);
        @Override RetryableWriteBatchCpTweaks allowInlineSsdAccess(boolean v);
        @Override RetryableWriteBatchCpTweaks useDurableDelete(boolean b);
        @Override RetryableWriteBatchCpTweaks simulateXdrWrite(boolean b);
    }

    public interface NonRetryableWriteAnyModeTweaks extends NonRetryableWriteTweaks {
        @Override NonRetryableWriteAnyModeTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWriteAnyModeTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWriteAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWriteAnyModeTweaks replicaOrder(Replica r);
        @Override NonRetryableWriteAnyModeTweaks sendKey(boolean sendKey);
        @Override NonRetryableWriteAnyModeTweaks useCompression(boolean compress);
        @Override NonRetryableWriteAnyModeTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWriteAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWriteAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWriteAnyModeTweaks useDurableDelete(boolean b);
        @Override NonRetryableWriteAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface NonRetryableWritePointAnyModeTweaks extends NonRetryableWriteTweaks {
        @Override NonRetryableWritePointAnyModeTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWritePointAnyModeTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWritePointAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWritePointAnyModeTweaks replicaOrder(Replica r);
        @Override NonRetryableWritePointAnyModeTweaks sendKey(boolean sendKey);
        @Override NonRetryableWritePointAnyModeTweaks useCompression(boolean compress);
        @Override NonRetryableWritePointAnyModeTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWritePointAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWritePointAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWritePointAnyModeTweaks useDurableDelete(boolean b);
        @Override NonRetryableWritePointAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface NonRetryableWritePointApTweaks extends WriteApTweaks {
        @Override NonRetryableWritePointApTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWritePointApTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWritePointApTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWritePointApTweaks replicaOrder(Replica r);
        @Override NonRetryableWritePointApTweaks sendKey(boolean sendKey);
        @Override NonRetryableWritePointApTweaks useCompression(boolean compress);
        @Override NonRetryableWritePointApTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWritePointApTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWritePointApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWritePointApTweaks useDurableDelete(boolean b);
        @Override NonRetryableWritePointApTweaks simulateXdrWrite(boolean b);
        @Override NonRetryableWritePointApTweaks commitLevel(CommitLevel level);
    }
    public interface NonRetryableWritePointCpTweaks extends NonRetryableWriteTweaks {
        @Override NonRetryableWritePointCpTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWritePointCpTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWritePointCpTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWritePointCpTweaks replicaOrder(Replica r);
        @Override NonRetryableWritePointCpTweaks sendKey(boolean sendKey);
        @Override NonRetryableWritePointCpTweaks useCompression(boolean compress);
        @Override NonRetryableWritePointCpTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWritePointCpTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWritePointCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWritePointCpTweaks useDurableDelete(boolean b);
        @Override NonRetryableWritePointCpTweaks simulateXdrWrite(boolean b);
    }
    public interface NonRetryableWriteBatchAnyModeTweaks extends BatchTweaks, NonRetryableWriteTweaks {
        @Override NonRetryableWriteBatchAnyModeTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWriteBatchAnyModeTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWriteBatchAnyModeTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWriteBatchAnyModeTweaks replicaOrder(Replica r);
        @Override NonRetryableWriteBatchAnyModeTweaks sendKey(boolean sendKey);
        @Override NonRetryableWriteBatchAnyModeTweaks useCompression(boolean compress);
        @Override NonRetryableWriteBatchAnyModeTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWriteBatchAnyModeTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWriteBatchAnyModeTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWriteBatchAnyModeTweaks maxConcurrentNodes(int n);
        @Override NonRetryableWriteBatchAnyModeTweaks allowInlineMemoryAccess(boolean v);
        @Override NonRetryableWriteBatchAnyModeTweaks allowInlineSsdAccess(boolean v);
        @Override NonRetryableWriteBatchAnyModeTweaks useDurableDelete(boolean b);
        @Override NonRetryableWriteBatchAnyModeTweaks simulateXdrWrite(boolean b);
    }
    public interface NonRetryableWriteBatchApTweaks extends BatchTweaks, WriteApTweaks {
        @Override NonRetryableWriteBatchApTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWriteBatchApTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWriteBatchApTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWriteBatchApTweaks replicaOrder(Replica r);
        @Override NonRetryableWriteBatchApTweaks sendKey(boolean sendKey);
        @Override NonRetryableWriteBatchApTweaks useCompression(boolean compress);
        @Override NonRetryableWriteBatchApTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWriteBatchApTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWriteBatchApTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWriteBatchApTweaks maxConcurrentNodes(int n);
        @Override NonRetryableWriteBatchApTweaks allowInlineMemoryAccess(boolean v);
        @Override NonRetryableWriteBatchApTweaks allowInlineSsdAccess(boolean v);
        @Override NonRetryableWriteBatchApTweaks useDurableDelete(boolean b);
        @Override NonRetryableWriteBatchApTweaks simulateXdrWrite(boolean b);
        @Override NonRetryableWriteBatchApTweaks commitLevel(CommitLevel level);
    }
    public interface NonRetryableWriteBatchCpTweaks extends BatchTweaks, NonRetryableWriteTweaks {
        @Override NonRetryableWriteBatchCpTweaks abandonCallAfter(Duration d);
        @Override NonRetryableWriteBatchCpTweaks delayBetweenRetries(Duration d);
        @Override NonRetryableWriteBatchCpTweaks maximumNumberOfCallAttempts(int n);
        @Override NonRetryableWriteBatchCpTweaks replicaOrder(Replica r);
        @Override NonRetryableWriteBatchCpTweaks sendKey(boolean sendKey);
        @Override NonRetryableWriteBatchCpTweaks useCompression(boolean compress);
        @Override NonRetryableWriteBatchCpTweaks waitForCallToComplete(Duration d);
        @Override NonRetryableWriteBatchCpTweaks waitForConnectionToComplete(Duration d);
        @Override NonRetryableWriteBatchCpTweaks waitForSocketResponseAfterCallFails(Duration d);
        @Override NonRetryableWriteBatchCpTweaks maxConcurrentNodes(int n);
        @Override NonRetryableWriteBatchCpTweaks allowInlineMemoryAccess(boolean v);
        @Override NonRetryableWriteBatchCpTweaks allowInlineSsdAccess(boolean v);
        @Override NonRetryableWriteBatchCpTweaks useDurableDelete(boolean b);
        @Override NonRetryableWriteBatchCpTweaks simulateXdrWrite(boolean b);
    }

    // -----------------------------------------------------------------------------------
    // Selectors + factories
    // -----------------------------------------------------------------------------------
    public interface Selector<T extends TweaksView> { SelectionSpec spec(); }

    /**
     * Factory for creating selectors that specify which operations to configure.
     * 
     * <p>Selectors use a fluent API to narrow down operation types from general to specific,
     * exposing only the configuration methods that are valid for each operation type.
     * 
     * <h2>Selector Hierarchy</h2>
     * 
     * <h3>All Operations:</h3>
     * <pre>{@code
     * Selectors.all()  // Configures ALL operations (reads + writes, all shapes, all modes)
     * }</pre>
     * 
     * <h3>Read Operations:</h3>
     * <pre>{@code
     * // By shape:
     * Selectors.reads()         // All reads, any shape
     * Selectors.reads().get()   // Single-record reads (POINT)
     * Selectors.reads().batch() // Multi-record reads (BATCH)
     * Selectors.reads().query() // Query/scan operations (QUERY)
     * 
     * // By mode:
     * Selectors.reads().ap()    // All AP-mode reads
     * Selectors.reads().cp()    // All CP-mode reads
     * 
     * // Combined (recommended order: shape → mode):
     * Selectors.reads().batch().ap()  // Batch reads in AP mode (exposes readMode, maxConcurrentNodes)
     * Selectors.reads().query().cp()  // Queries in CP mode (exposes consistency, recordQueueSize)
     * }</pre>
     * 
     * <h3>Write Operations:</h3>
     * <pre>{@code
     * // By shape (applies to both retryable and non-retryable):
     * Selectors.writes().point()  // All single-record writes
     * Selectors.writes().batch()  // All multi-record writes
     * 
     * // By retryability:
     * Selectors.writes().retryable()     // Retryable writes (puts, updates)
     * Selectors.writes().nonRetryable()  // Non-retryable writes (deletes, operations)
     * 
     * // Combined - shape then mode (recommended for retryability-agnostic):
     * Selectors.writes().point().ap()    // All point writes in AP mode (exposes commitLevel)
     * Selectors.writes().batch().cp()    // All batch writes in CP mode
     * 
     * // Combined - retryability then shape (more specific):
     * Selectors.writes().retryable().point()   // Only retryable point writes
     * Selectors.writes().nonRetryable().batch() // Only non-retryable batch writes
     * 
     * // Combined - retryability then shape then mode:
     * Selectors.writes().retryable().point().ap()       // Retryable point writes in AP
     * Selectors.writes().nonRetryable().batch().cp()    // Non-retryable batch writes in CP
     * 
     * // By mode (applies to all shapes and retryability):
     * Selectors.writes().ap()  // All writes in AP mode
     * Selectors.writes().cp()  // All writes in CP mode
     * }</pre>
     * 
     * <h2>Configuration Precedence</h2>
     * 
     * Settings cascade from general to specific (last-writer wins):
     * <ol>
     *   <li>Parent behavior (via defaultsFrom or deriveWithChanges)</li>
     *   <li>Selectors.all() - applies to everything</li>
     *   <li>Kind level - Selectors.reads() or Selectors.writes()</li>
     *   <li>Retryability - .retryable() or .nonRetryable() (writes only)</li>
     *   <li>Shape level - .get(), .batch(), .query()</li>
     *   <li>Mode level - .ap() or .cp()</li>
     * </ol>
     * 
     * <h2>Examples</h2>
     * 
     * <h3>Configuring all operations:</h3>
     * <pre>{@code
     * builder.on(Selectors.all(), ops -> ops
     *     .abandonCallAfter(Duration.ofSeconds(30))
     *     .maximumNumberOfCallAttempts(3)
     * );
     * }</pre>
     * 
     * <h3>Configuring specific read types:</h3>
     * <pre>{@code
     * builder
     *     // All AP reads use ALL replicas
     *     .on(Selectors.reads().ap(), ops -> ops
     *         .readMode(ReadModeAP.ALL)
     *     )
     *     // Batch reads have high concurrency
     *     .on(Selectors.reads().batch().ap(), ops -> ops
     *         .maxConcurrentNodes(16)
     *         .allowInlineMemoryAccess(true)
     *     )
     *     // CP queries need strong consistency
     *     .on(Selectors.reads().query().cp(), ops -> ops
     *         .consistency(ReadConsistency.LINEARIZABLE)
     *         .recordQueueSize(10000)
     *     );
     * }</pre>
     * 
     * <h3>Configuring write operations:</h3>
     * <pre>{@code
     * builder
     *     // All batch writes (retryable AND non-retryable)
     *     .on(Selectors.writes().batch(), ops -> ops
     *         .maxConcurrentNodes(8)
     *         .allowInlineMemoryAccess(true)
     *     )
     *     // All point writes in AP mode (exposes commitLevel)
     *     .on(Selectors.writes().point().ap(), ops -> ops
     *         .commitLevel(CommitLevel.COMMIT_ALL)
     *     )
     *     // All CP writes use durable delete
     *     .on(Selectors.writes().cp(), ops -> ops
     *         .useDurableDelete(true)
     *     )
     *     // Only retryable writes have more retry attempts
     *     .on(Selectors.writes().retryable(), ops -> ops
     *         .maximumNumberOfCallAttempts(5)
     *     )
     *     // Only non-retryable point writes
     *     .on(Selectors.writes().nonRetryable().point(), ops -> ops
     *         .maximumNumberOfCallAttempts(1)
     *     );
     * }</pre>
     * 
     * <h2>Best Practices</h2>
     * <ul>
     *   <li><b>Order matters for type safety:</b> Select mode last for best compile-time checking
     *       <br>✓ {@code Selectors.writes().retryable().point().ap()} (commitLevel visible)
     *       <br>⚠ {@code Selectors.writes().ap().retryable().point()} (commitLevel not visible, but works)</li>
     *   <li><b>Start broad, then narrow:</b> Configure common settings with .all(), override for specifics</li>
     *   <li><b>Use meaningful names:</b> Name behaviors descriptively (e.g., "production", "highLoad")</li>
     *   <li><b>Build hierarchies:</b> Use deriveWithChanges to create environment-specific configurations</li>
     * </ul>
     * 
     * @see Behavior.BehaviorBuilder#on(Selector, java.util.function.Consumer)
     * @see Behavior#deriveWithChanges(String, java.util.function.Consumer)
     */
    public static final class Selectors {
        private Selectors() {}
        
        /**
         * Selects ALL operations (reads, writes, all shapes, all modes).
         * Use this for settings that should apply universally.
         * 
         * @return selector for all operations
         */
        public static AllSelector all() { return new AllSelector(new SelectionSpec(null, OpShape.ANY, Mode.ANY)); }
        
        /**
         * Selects all READ operations. Continue chaining to narrow by shape or mode.
         * 
         * @return selector for read operations
         */
        public static ReadAnySelector<ReadAnyAnyModeTweaks> reads() { return new ReadAnySel<>(new SelectionSpec(OpKind.READ, OpShape.ANY, Mode.ANY)); }
        
        /**
         * Selects all WRITE operations. Continue chaining to narrow by retryability, shape, or mode.
         * 
         * @return selector for write operations
         */
        public static WriteRootSelector<WriteRootAnyModeTweaks> writes() { return new WriteRootSel(new SelectionSpec(null, OpShape.ANY, Mode.ANY)); }
    }

    public static final class AllSelector implements Selector<AllAnyModeTweaks> {
        private final SelectionSpec spec;
        AllSelector(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
    }

    // READ selectors (shape → mode chaining)
    public interface ReadAnySelector<T extends TweaksView> extends Selector<T> {
        ReadPointSelector<ReadPointAnyModeTweaks> get();
        ReadBatchSelector<ReadBatchAnyModeTweaks> batch();
        ReadQuerySelector<ReadQueryAnyModeTweaks> query();

        // Mode shortcuts at 'any-shape' level expose mode-specific knobs
        ReadAnySelector<ReadAnyApTweaks> ap();
        ReadAnySelector<ReadAnyCpTweaks> cp();
    }
    public interface ReadPointSelector<T extends TweaksView> extends Selector<T> {
        ReadPointSelector<ReadPointApTweaks> ap();
        ReadPointSelector<ReadPointCpTweaks> cp();
    }
    public interface ReadBatchSelector<T extends TweaksView> extends Selector<T> {
        ReadBatchSelector<ReadBatchApTweaks> ap();
        ReadBatchSelector<ReadBatchCpTweaks> cp();
    }
    public interface ReadQuerySelector<T extends TweaksView> extends Selector<T> {
        ReadQuerySelector<ReadQueryApTweaks> ap();
        ReadQuerySelector<ReadQueryCpTweaks> cp();
    }

    static final class ReadAnySel<T extends TweaksView> implements ReadAnySelector<T> {
        private final SelectionSpec spec;
        ReadAnySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        @Override public ReadPointSelector<ReadPointAnyModeTweaks> get()  { return new ReadPointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public ReadBatchSelector<ReadBatchAnyModeTweaks> batch(){ return new ReadBatchSel<>(spec.withShape(OpShape.BATCH)); }
        @Override public ReadQuerySelector<ReadQueryAnyModeTweaks> query(){ return new ReadQuerySel<>(spec.withShape(OpShape.QUERY)); }

        @Override public ReadAnySelector<ReadAnyApTweaks> ap() { return new ReadAnySel<>(spec.withMode(Mode.AP)); }
        @Override public ReadAnySelector<ReadAnyCpTweaks> cp() { return new ReadAnySel<>(spec.withMode(Mode.CP)); }
    }
    static final class ReadPointSel<T extends TweaksView> implements ReadPointSelector<T> {
        private final SelectionSpec spec;
        ReadPointSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public ReadPointSelector<ReadPointApTweaks> ap() { return new ReadPointSel<>(spec.withMode(Mode.AP)); }
        @Override public ReadPointSelector<ReadPointCpTweaks> cp() { return new ReadPointSel<>(spec.withMode(Mode.CP)); }
    }
    static final class ReadBatchSel<T extends TweaksView> implements ReadBatchSelector<T> {
        private final SelectionSpec spec;
        ReadBatchSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public ReadBatchSelector<ReadBatchApTweaks> ap() { return new ReadBatchSel<>(spec.withMode(Mode.AP)); }
        @Override public ReadBatchSelector<ReadBatchCpTweaks> cp() { return new ReadBatchSel<>(spec.withMode(Mode.CP)); }
    }
    static final class ReadQuerySel<T extends TweaksView> implements ReadQuerySelector<T> {
        private final SelectionSpec spec;
        ReadQuerySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public ReadQuerySelector<ReadQueryApTweaks> ap() { return new ReadQuerySel<>(spec.withMode(Mode.AP)); }
        @Override public ReadQuerySelector<ReadQueryCpTweaks> cp() { return new ReadQuerySel<>(spec.withMode(Mode.CP)); }
    }

    // WRITE selectors (shape → mode and retryable toggle)
    /**
     * Root selector for write operations. Allows selection of mode, retryability, and shape.
     * 
     * <h3>Selector Ordering Patterns</h3>
     * 
     * <p><b>Recommended Pattern (Retryability → Shape → Mode):</b>
     * <pre>{@code
     * // Best type safety - mode-specific methods available in final type
     * Selectors.writes().retryable().point().ap()    // Returns RetryableWritePointApTweaks with commitLevel()
     * Selectors.writes().retryable().batch().cp()    // Returns RetryableWriteBatchCpTweaks
     * Selectors.writes().nonRetryable().point().cp() // Returns NonRetryableWritePointCpTweaks
     * }</pre>
     * 
     * <p><b>Alternative Pattern (Mode → Retryability → Shape):</b>
     * <pre>{@code
     * // Works at runtime, but loses type-specific methods in intermediate steps
     * Selectors.writes().ap().retryable().point()    // ⚠ Returns RetryableWritePointAnyModeTweaks (no commitLevel visible)
     * // However, the mode IS correctly set internally and will be applied
     * }</pre>
     * 
     * <p><b>Why Order Matters:</b></p>
     * <ul>
     *   <li>Java's type system resolves types at compile time based on method return types</li>
     *   <li>When mode is selected LAST, the final type includes mode-specific methods (e.g., commitLevel() for AP)</li>
     *   <li>When mode is selected FIRST, subsequent selectors return generic types that don't expose mode-specific methods</li>
     *   <li>Runtime behavior is identical - the SelectionSpec correctly captures all selections regardless of order</li>
     * </ul>
     * 
     * <p><b>Type Safety vs Runtime Behavior:</b></p>
     * Both patterns produce the same runtime result. The difference is compile-time type checking:
     * <ul>
     *   <li>Recommended pattern: Compiler enforces that you only call methods valid for the selected mode</li>
     *   <li>Alternative pattern: Mode-specific methods aren't visible, but settings are still applied correctly</li>
     * </ul>
     */
    public interface WriteRootSelector<T extends TweaksView> extends Selector<T> {
        WriteRootSelector<WriteRootApTweaks> ap();
        WriteRootSelector<WriteRootCpTweaks> cp();
        RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable();
        NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable();
        
        // Shape selection without retryability (applies to both retryable and non-retryable)
        WritePointSelector<WritePointAnyModeTweaks> point();
        WriteBatchSelector<WriteBatchAnyModeTweaks> batch();
    }
    public interface RetryableWriteSelector<T extends TweaksView> extends Selector<T> {
        RetryableWriteSelector<T> ap();
        RetryableWriteSelector<T> cp();
        RetryableWritePointSelector<RetryableWritePointAnyModeTweaks> point();
        RetryableWriteBatchSelector<RetryableWriteBatchAnyModeTweaks> batch();
    }
    public interface RetryableWritePointSelector<T extends TweaksView> extends Selector<T> {
        RetryableWritePointSelector<RetryableWritePointApTweaks> ap();
        RetryableWritePointSelector<RetryableWritePointCpTweaks> cp();
    }
    public interface RetryableWriteBatchSelector<T extends TweaksView> extends Selector<T> {
        RetryableWriteBatchSelector<RetryableWriteBatchApTweaks> ap();
        RetryableWriteBatchSelector<RetryableWriteBatchCpTweaks> cp();
    }
    public interface NonRetryableWriteSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWriteSelector<T> ap();
        NonRetryableWriteSelector<T> cp();
        NonRetryableWritePointSelector<NonRetryableWritePointAnyModeTweaks> point();
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchAnyModeTweaks> batch();
    }
    public interface NonRetryableWritePointSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWritePointSelector<NonRetryableWritePointApTweaks> ap();
        NonRetryableWritePointSelector<NonRetryableWritePointCpTweaks> cp();
    }
    public interface NonRetryableWriteBatchSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchApTweaks> ap();
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchCpTweaks> cp();
    }

    // Write shape selectors (retryability-agnostic - apply to both retryable and non-retryable)
    public interface WritePointSelector<T extends TweaksView> extends Selector<T> {
        WritePointSelector<WritePointApTweaks> ap();
        WritePointSelector<WritePointCpTweaks> cp();
    }
    public interface WriteBatchSelector<T extends TweaksView> extends Selector<T> {
        WriteBatchSelector<WriteBatchApTweaks> ap();
        WriteBatchSelector<WriteBatchCpTweaks> cp();
    }

    public static final class WriteRootSel implements WriteRootSelector<WriteRootAnyModeTweaks> {
        private final SelectionSpec spec;
        WriteRootSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        // Mode-first pattern: returns mode-specific selector types (WriteRootApTweaks, WriteRootCpTweaks)
        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return new WriteRootApSel(spec.withMode(Mode.AP)); }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return new WriteRootCpSel(spec.withMode(Mode.CP)); }

        // Retryability selection: returns generic "AnyMode" types
        // Note: If mode was already set via .ap()/.cp(), the SelectionSpec carries it forward,
        // but the return type doesn't reflect mode-specific methods (type system limitation)
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }
        
        // Shape selection (retryability-agnostic - applies to both retryable and non-retryable)
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH)); }
    }
    public static final class WriteRootApSel implements WriteRootSelector<WriteRootApTweaks> {
        private final SelectionSpec spec;
        WriteRootApSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return this; }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return new WriteRootCpSel(spec.withMode(Mode.CP)); }

        // These return generic types, but the AP mode is preserved in the SelectionSpec
        // Limitation: subsequent selector chain won't expose AP-specific methods like commitLevel()
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }
        
        // Shape selection (retryability-agnostic) - AP mode preserved in SelectionSpec
        // Note: returns generic AnyMode types, but can still call .ap() afterward to expose AP-specific methods
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH)); }
    }
    public static final class WriteRootCpSel implements WriteRootSelector<WriteRootCpTweaks> {
        private final SelectionSpec spec;
        WriteRootCpSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return new WriteRootApSel(spec.withMode(Mode.AP)); }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return this; }

        // These return generic types, but the CP mode is preserved in the SelectionSpec
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }
        
        // Shape selection (retryability-agnostic) - CP mode preserved in SelectionSpec
        // Note: returns generic AnyMode types, but can still call .cp() afterward to expose CP-specific methods
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH)); }
    }
    public static final class RetryableWriteAnySel implements RetryableWriteSelector<RetryableWriteAnyModeTweaks> {
        private final SelectionSpec spec;
        RetryableWriteAnySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> ap() { return new RetryableWriteAnySel(spec.withMode(Mode.AP)); }
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> cp() { return new RetryableWriteAnySel(spec.withMode(Mode.CP)); }

        @Override public RetryableWritePointSelector<RetryableWritePointAnyModeTweaks> point() { return new RetryableWritePointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchAnyModeTweaks> batch() { return new RetryableWriteBatchSel<>(spec.withShape(OpShape.BATCH)); }
    }
    public static final class RetryableWritePointSel<T extends TweaksView> implements RetryableWritePointSelector<T> {
        private final SelectionSpec spec;
        RetryableWritePointSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public RetryableWritePointSelector<RetryableWritePointApTweaks> ap() { return new RetryableWritePointSel<>(spec.withMode(Mode.AP)); }
        @Override public RetryableWritePointSelector<RetryableWritePointCpTweaks> cp() { return new RetryableWritePointSel<>(spec.withMode(Mode.CP)); }
    }
    public static final class RetryableWriteBatchSel<T extends TweaksView> implements RetryableWriteBatchSelector<T> {
        private final SelectionSpec spec;
        RetryableWriteBatchSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchApTweaks> ap() { return new RetryableWriteBatchSel<>(spec.withMode(Mode.AP)); }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchCpTweaks> cp() { return new RetryableWriteBatchSel<>(spec.withMode(Mode.CP)); }
    }
    public static final class NonRetryableWriteAnySel implements NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> {
        private final SelectionSpec spec;
        NonRetryableWriteAnySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }

        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> ap() { return new NonRetryableWriteAnySel(spec.withMode(Mode.AP)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> cp() { return new NonRetryableWriteAnySel(spec.withMode(Mode.CP)); }

        @Override public NonRetryableWritePointSelector<NonRetryableWritePointAnyModeTweaks> point() { return new NonRetryableWritePointSel<>(spec.withShape(OpShape.POINT)); }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchAnyModeTweaks> batch() { return new NonRetryableWriteBatchSel<>(spec.withShape(OpShape.BATCH)); }
    }
    public static final class NonRetryableWritePointSel<T extends TweaksView> implements NonRetryableWritePointSelector<T> {
        private final SelectionSpec spec;
        NonRetryableWritePointSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public NonRetryableWritePointSelector<NonRetryableWritePointApTweaks> ap() { return new NonRetryableWritePointSel<>(spec.withMode(Mode.AP)); }
        @Override public NonRetryableWritePointSelector<NonRetryableWritePointCpTweaks> cp() { return new NonRetryableWritePointSel<>(spec.withMode(Mode.CP)); }
    }
    public static final class NonRetryableWriteBatchSel<T extends TweaksView> implements NonRetryableWriteBatchSelector<T> {
        private final SelectionSpec spec;
        NonRetryableWriteBatchSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchApTweaks> ap() { return new NonRetryableWriteBatchSel<>(spec.withMode(Mode.AP)); }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchCpTweaks> cp() { return new NonRetryableWriteBatchSel<>(spec.withMode(Mode.CP)); }
    }

    // Write shape selector implementations (retryability-agnostic)
    static final class WritePointSel<T extends TweaksView> implements WritePointSelector<T> {
        private final SelectionSpec spec;
        WritePointSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public WritePointSelector<WritePointApTweaks> ap() { return new WritePointSel<>(spec.withMode(Mode.AP)); }
        @Override public WritePointSelector<WritePointCpTweaks> cp() { return new WritePointSel<>(spec.withMode(Mode.CP)); }
    }
    static final class WriteBatchSel<T extends TweaksView> implements WriteBatchSelector<T> {
        private final SelectionSpec spec;
        WriteBatchSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public WriteBatchSelector<WriteBatchApTweaks> ap() { return new WriteBatchSel<>(spec.withMode(Mode.AP)); }
        @Override public WriteBatchSelector<WriteBatchCpTweaks> cp() { return new WriteBatchSel<>(spec.withMode(Mode.CP)); }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks proxy (records into Patch) — returns TweaksProxy (covariant for all)
    // -----------------------------------------------------------------------------------
    static final class TweaksProxy implements
    // base capability interfaces
    CommonTweaks, BatchTweaks, QueryTweaks, ReadApTweaks, ReadCpTweaks, WriteApTweaks, RetryableWriteTweaks, NonRetryableWriteTweaks,
    // concrete any-mode & read views
    AllAnyModeTweaks, ReadAnyAnyModeTweaks, ReadAnyApTweaks, ReadAnyCpTweaks,
    ReadPointAnyModeTweaks, ReadBatchAnyModeTweaks, ReadQueryAnyModeTweaks,
    ReadPointApTweaks, ReadPointCpTweaks, ReadBatchApTweaks, ReadBatchCpTweaks, ReadQueryApTweaks, ReadQueryCpTweaks,
    // write views (retryability-agnostic)
    WritePointAnyModeTweaks, WritePointApTweaks, WritePointCpTweaks,
    WriteBatchAnyModeTweaks, WriteBatchApTweaks, WriteBatchCpTweaks,
    // write views (root)
    WriteRootAnyModeTweaks, WriteRootApTweaks, WriteRootCpTweaks,
    // write views (retryable)
    RetryableWriteAnyModeTweaks, RetryableWritePointAnyModeTweaks, RetryableWriteBatchAnyModeTweaks,
    RetryableWriteBatchApTweaks, RetryableWriteBatchCpTweaks, RetryableWritePointApTweaks, RetryableWritePointCpTweaks,
    // write views (non-retryable)
    NonRetryableWriteAnyModeTweaks, NonRetryableWritePointAnyModeTweaks, NonRetryableWriteBatchAnyModeTweaks,
    NonRetryableWriteBatchApTweaks, NonRetryableWriteBatchCpTweaks, NonRetryableWritePointApTweaks, NonRetryableWritePointCpTweaks {

        private final Patch patch;
        TweaksProxy(Patch patch) { this.patch = patch; }

        // Common
        @Override public TweaksProxy abandonCallAfter(Duration d) { patch.settings.abandonCallAfter = d; return this; }
        @Override public TweaksProxy delayBetweenRetries(Duration d) { patch.settings.delayBetweenRetries = d; return this; }
        @Override public TweaksProxy maximumNumberOfCallAttempts(int n) { patch.settings.maximumNumberOfCallAttempts = n; return this; }
        @Override public TweaksProxy replicaOrder(Replica r) { patch.settings.replicaOrder = r; return this; }
        @Override public TweaksProxy sendKey(boolean sendKey) { patch.settings.sendKey = sendKey; return this; }
        @Override public TweaksProxy useCompression(boolean compress) { patch.settings.useCompression = compress; return this; }
        @Override public TweaksProxy waitForCallToComplete(Duration d) { patch.settings.waitForCallToComplete = d; return this; }
        @Override public TweaksProxy waitForConnectionToComplete(Duration d) { patch.settings.waitForConnectionToComplete = d; return this; }
        @Override public TweaksProxy waitForSocketResponseAfterCallFails(Duration d) { patch.settings.waitForSocketResponseAfterCallFails = d; return this; }

        // Query
        @Override public TweaksProxy recordQueueSize(int n) { patch.settings.recordQueueSize = n; return this; }

        // Batch
        @Override public TweaksProxy maxConcurrentNodes(int n) { patch.settings.maxConcurrentNodes = n; return this; }
        @Override public TweaksProxy allowInlineMemoryAccess(boolean v) { patch.settings.allowInlineMemoryAccess = v; return this; }
        @Override public TweaksProxy allowInlineSsdAccess(boolean v) { patch.settings.allowInlineSsdAccess = v; return this; }

        // Write
        @Override public TweaksProxy useDurableDelete(boolean b) { patch.settings.useDurableDelete = b; return this; }
        @Override public TweaksProxy simulateXdrWrite(boolean b) { patch.settings.simulateXdrWrite = b; return this; }

        // Write AP
        @Override public TweaksProxy commitLevel(CommitLevel level) { patch.settings.commitLevel = level; return this; }

        // Read
        @Override public TweaksProxy resetTtlOnReadAtPercent(int percent) { patch.settings.resetTtlOnReadAtPercent = percent; return this; }

        // Read modes
        @Override public TweaksProxy readMode(ReadModeAP mode) { patch.settings.readModeAP = mode; return this; }
        @Override public TweaksProxy consistency(ReadModeSC c) { patch.settings.readModeSC = c; return this; }
    }

    // -----------------------------------------------------------------------------------
    // Example usage (lambda style) + full selector coverage test with parent inheritance
    // -----------------------------------------------------------------------------------
    
    /**
     * Demonstrates selector usage patterns and configuration hierarchy.
     * 
     * <h3>Selector Pattern Examples:</h3>
     * This method shows both recommended and alternative selector patterns.
     */
    public static Behavior example() {
        // Parent defaults to test parent-level inheritance
        Behavior parent = Behavior.builder("parentDefaults")
                .on(Selectors.all(), ops -> ops
                        .waitForSocketResponseAfterCallFails(Duration.ofSeconds(5))
                        .waitForCallToComplete(Duration.ofMillis(50))
                        )
                .on(Selectors.reads().batch(), ops -> ops
                        .maxConcurrentNodes(2)
                        )
                .on(Selectors.reads().cp(), ops -> ops
                        .consistency(ReadModeSC.SESSION)
                        )
                .build();

        // Using the new deriveWithChanges API (recommended)
        Behavior child = parent.deriveWithChanges("childOverrides", builder -> builder

                // Global sweep (applies to all operations)
                .on(Selectors.all(), ops -> ops
                        .waitForSocketResponseAfterCallFails(Duration.ofSeconds(3))
                        )

                // Reads sweep (any-shape, AP-only readMode shortcut)
                .on(Selectors.reads().ap(), ops -> ops
                        .readMode(ReadModeAP.ALL)
                        .waitForCallToComplete(Duration.ofMillis(25))
                        .abandonCallAfter(Duration.ofMillis(100))
                        .maximumNumberOfCallAttempts(3)
                        )

                // Reads sweep (any-shape, CP-only consistency)
                .on(Selectors.reads().cp(), ops -> ops
                        .consistency(ReadModeSC.LINEARIZE)
                        )

                // Shape-specific any-mode
                .on(Selectors.reads().get(),   ops -> ops.maximumNumberOfCallAttempts(4))
                .on(Selectors.reads().batch(), ops -> ops.maximumNumberOfCallAttempts(7).allowInlineMemoryAccess(true))
                .on(Selectors.reads().query(), ops -> ops.maximumNumberOfCallAttempts(2))

                // Most-specific read overrides
                .on(Selectors.reads().get().ap(),    ops -> ops.readMode(ReadModeAP.ONE))
                .on(Selectors.reads().get().cp(),    ops -> ops.consistency(ReadModeSC.SESSION))
                .on(Selectors.reads().batch().ap(),  ops -> ops.readMode(ReadModeAP.ALL).maxConcurrentNodes(4))
                .on(Selectors.reads().batch().cp(),  ops -> ops.consistency(ReadModeSC.ALLOW_REPLICA).maxConcurrentNodes(3))
                .on(Selectors.reads().query().ap(),  ops -> ops.readMode(ReadModeAP.ONE))
                .on(Selectors.reads().query().cp(),  ops -> ops.consistency(ReadModeSC.LINEARIZE))

                // WRITE SELECTOR PATTERN EXAMPLES:
                
                // ✓ RECOMMENDED: Mode selection first works for broad settings
                .on(Selectors.writes().ap(), ops -> ops.abandonCallAfter(Duration.ofMillis(80)))
                .on(Selectors.writes().cp(), ops -> ops.waitForCallToComplete(Duration.ofMillis(40)))
                
                // ✓ RECOMMENDED PATTERN: Retryability → Shape → Mode
                // This pattern exposes mode-specific methods (like commitLevel) in the final type
                .on(Selectors.writes().retryable().point().ap(), ops -> ops
                        .maximumNumberOfCallAttempts(9)
                        // .commitLevel(CommitLevel.COMMIT_ALL)  // ✓ commitLevel() is visible and type-safe
                )
                .on(Selectors.writes().retryable().point().cp(), ops -> ops.maximumNumberOfCallAttempts(8))
                .on(Selectors.writes().retryable().batch().ap(), ops -> ops.maximumNumberOfCallAttempts(7).maxConcurrentNodes(5))
                .on(Selectors.writes().retryable().batch().cp(), ops -> ops.maximumNumberOfCallAttempts(6).maxConcurrentNodes(4))
                
                // ⚠ ALTERNATIVE PATTERN: Mode → Retryability → Shape
                // This also works at runtime, but the final type doesn't expose mode-specific methods
                // Uncomment to see - it compiles and works correctly, mode is applied:
                // .on(Selectors.writes().ap().retryable().point(), ops -> ops
                //         .maximumNumberOfCallAttempts(9)
                //         // .commitLevel(CommitLevel.COMMIT_ALL)  // ⚠ Would not compile - commitLevel() not visible
                // )

                // Non-retryable writes
                .on(Selectors.writes().nonRetryable().point().ap(), ops -> ops.maximumNumberOfCallAttempts(5))
                .on(Selectors.writes().nonRetryable().point().cp(), ops -> ops.maximumNumberOfCallAttempts(4))
                .on(Selectors.writes().nonRetryable().batch().ap(), ops -> ops.maximumNumberOfCallAttempts(3).maxConcurrentNodes(2))
                .on(Selectors.writes().nonRetryable().batch().cp(), ops -> ops.maximumNumberOfCallAttempts(2).maxConcurrentNodes(1))
        );

        // You can print child.explain() in a test to see the full resolution
        // System.out.println(child.explain());
        return child;
    }


    public static void main(String[] args) {
        System.out.println(Behavior.DEFAULT.explain());
        System.out.println();
        
        // Demonstrate getSettings() method
        System.out.println("=== Testing getSettings() ===");
        Settings readBatchAp = Behavior.DEFAULT.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        System.out.println("READ:BATCH:AP settings:");
        System.out.println("  maxConcurrentNodes: " + readBatchAp.maxConcurrentNodes);
        System.out.println("  allowInlineMemoryAccess: " + readBatchAp.allowInlineMemoryAccess);
        System.out.println("  readModeAP: " + readBatchAp.readModeAP);
        System.out.println("  abandonCallAfter: " + readBatchAp.abandonCallAfter);
        System.out.println();
        
        Settings writeRetryableCp = Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
        System.out.println("WRITE_RETRYABLE:POINT:CP settings:");
        System.out.println("  maximumNumberOfCallAttempts: " + writeRetryableCp.maximumNumberOfCallAttempts);
        System.out.println("  useDurableDelete: " + writeRetryableCp.useDurableDelete);
        System.out.println("  abandonCallAfter: " + writeRetryableCp.abandonCallAfter);
        
        // Test with custom behavior that inherits from DEFAULT
        System.out.println();
        System.out.println("=== Custom Behavior with Inheritance ===");
        
        // NEW API: deriveWithChanges (recommended pattern)
        Behavior custom = Behavior.DEFAULT.deriveWithChanges("custom", builder -> builder
                // EXAMPLE: Recommended pattern - mode last for type safety
                .on(Selectors.reads().batch().ap(), ops -> ops
                        .maxConcurrentNodes(8)
                        .abandonCallAfter(Duration.ofSeconds(10))
                        .readMode(ReadModeAP.ALL)  // ✓ readMode() is visible because we selected AP last
                )
        );
        
        // OLD API: builder pattern (still works, shown for comparison)
        // Behavior custom = Behavior.builder("custom")
        //         .defaultsFrom(Behavior.DEFAULT)
        //         .on(Selectors.reads().batch().ap(), ops -> ops
        //                 .maxConcurrentNodes(8)
        //                 .abandonCallAfter(Duration.ofSeconds(10))
        //                 .readMode(ReadModeAP.ALL)
        //         )
        //         .build();
        
        Settings customReadBatchAp = custom.getSettings(OpKind.READ, OpShape.BATCH, Mode.AP);
        System.out.println("Custom READ:BATCH:AP settings (showing overrides + inherited):");
        System.out.println("  maxConcurrentNodes: " + customReadBatchAp.maxConcurrentNodes + " (overridden)");
        System.out.println("  abandonCallAfter: " + customReadBatchAp.abandonCallAfter + " (overridden)");
        System.out.println("  allowInlineMemoryAccess: " + customReadBatchAp.allowInlineMemoryAccess + " (inherited from DEFAULT)");
        System.out.println("  readModeAP: " + customReadBatchAp.readModeAP + " (inherited from DEFAULT)");
        System.out.println("  sendKey: " + customReadBatchAp.sendKey + " (inherited from DEFAULT)");
        System.out.println(String.join(",\n",customReadBatchAp.toString().split(",")));

        Settings customWritePointCp = custom.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.CP);
        System.out.println("Custom WRITE:POINT:CP settings (showing overrides + inherited):");
        System.out.println(String.join(",\n",customWritePointCp.toString().split(",")));

        // NEW: Demonstrate retryability-agnostic write selectors
        System.out.println();
        System.out.println("=== Retryability-Agnostic Write Selectors ===");
        
        Behavior batchOptimized = Behavior.DEFAULT.deriveWithChanges("batchOptimized", builder -> builder
                // Configure ALL batch writes (retryable AND non-retryable)
                .on(Selectors.writes().batch(), ops -> ops
                        .maxConcurrentNodes(16)
                        .allowInlineMemoryAccess(true)
                )
                // Configure ALL point writes in AP mode
                .on(Selectors.writes().point().ap(), ops -> ops
                        .commitLevel(CommitLevel.COMMIT_ALL)
                )
        );
        
        System.out.println("Checking that .batch() applies to BOTH retryable and non-retryable:");
        Settings retryableBatch = batchOptimized.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP);
        Settings nonRetryableBatch = batchOptimized.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, Mode.AP);
        System.out.println("  Retryable batch maxConcurrentNodes: " + retryableBatch.maxConcurrentNodes + " (should be 16)");
        System.out.println("  Non-retryable batch maxConcurrentNodes: " + nonRetryableBatch.maxConcurrentNodes + " (should be 16)");
        System.out.println();
        
        System.out.println("Checking that .point().ap() exposes commitLevel and applies to both:");
        Settings retryablePoint = batchOptimized.getSettings(OpKind.WRITE_RETRYABLE, OpShape.POINT, Mode.AP);
        Settings nonRetryablePoint = batchOptimized.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, Mode.AP);
        System.out.println("  Retryable point commitLevel: " + retryablePoint.commitLevel + " (should be COMMIT_ALL)");
        System.out.println("  Non-retryable point commitLevel: " + nonRetryablePoint.commitLevel + " (should be COMMIT_ALL)");
    }
}
