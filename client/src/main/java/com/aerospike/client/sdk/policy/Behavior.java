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
package com.aerospike.client.sdk.policy;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.aerospike.client.sdk.ErrorDetailVerbosity;

/**
 * Aerospike Policy Behavior Builder — Typed Selectors
 *
 * <h2>Overview</h2>
 * Provides selector-driven API for configuring Aerospike operation policies with:
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
 * Selectors.reads().batch().ap()     // Exposes readMode(), maxConcurrentNodes(), etc.
 * Selectors.reads().query().cp()     // Exposes consistency(), recordQueueSize(), etc.
 *
 * // WRITES: kind → retryability → shape → mode
 * Selectors.writes().retryable().point().ap()    // Exposes commitLevel()
 * Selectors.writes().nonRetryable().batch().cp() // Exposes useDurableDelete()
 * }</pre>
 *
 * <h3>Alternative Pattern (Works but loses type safety):</h3>
 * Mode can be specified earlier, but intermediate steps lose type-specific methods:
 * <pre>{@code
 * // Mode selected first - works at runtime, but loses compile-time type safety
 * Selectors.writes().ap().retryable().point()  // ⚠ Works, but intermediate types don't expose commitLevel()
 *
 * // Recommended alternative: select mode last
 * Selectors.writes().retryable().point().ap()  // Full type safety throughout chain
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
 *   <li><b>OpKind:</b> READ, WRITE_RETRYABLE, WRITE_NON_RETRYABLE, SYSTEM_TXN_VERIFY, SYSTEM_TXN_ROLL</li>
 *   <li><b>OpShape:</b> POINT (single record), BATCH (multiple records), QUERY (scan with filter),
 *       SYSTEM (system-level operations)</li>
 *   <li><b>Mode:</b> AP (availability priority), CP (consistency priority)</li>
 * </ul>
 *
 * <h2>Transaction Configuration</h2>
 * The {@code transaction()} selector provides access to transaction-specific operations:
 * <ul>
 *   <li><b>txnVerify:</b> Transaction verification retry and consistency settings (read-like)</li>
 *   <li><b>txnRoll:</b> Transaction rollback retry settings (write-like)</li>
 * </ul>
 *
 * <p><b>Note:</b> System-level settings (connections, circuit breaker, refresh intervals) are no longer
 * configured via Behaviors. Use {@link com.aerospike.SystemSettings} and
 * {@link com.aerospike.ClusterDefinition#withSystemSettings(com.aerospike.SystemSettings)} instead.</p>
 *
 * <h3>Transaction Configuration Example:</h3>
 * <pre>{@code
 * Behavior customTxn = Behavior.DEFAULT.deriveWithChanges("customTxn", builder -> builder
 *     .on(Selectors.transaction().txnVerify(), ops -> ops
 *         .consistency(ReadModeSC.LINEARIZE)
 *         .maximumNumberOfCallAttempts(10)
 *     )
 *     .on(Selectors.transaction().txnRoll(), ops -> ops
 *         .allowInlineMemoryAccess(false)
 *         .maxConcurrentNodes(8)
 *     )
 * );
 * }</pre>
 *
 * <h2>Thread safety</h2>
 * <p>Resolved settings are exposed through a volatile matrix, so concurrent {@link #getSettings} reads are safe.
 * For profiles updated from YAML, the patch list is replaced in place under lock, then the matrix is rebuilt
 * so policy queries observe a consistent snapshot.</p>
 * <p>The {@link BehaviorBuilder} is not thread-safe.</p>
 *
 * @see #deriveWithChanges(String, java.util.function.Consumer)
 * @see #getSettings(OpKind, OpShape, Mode)
 * @see Selectors
 */
public final class Behavior {

    // -----------------------------------------------------------------------------------
    // Internal factory (package-private for DEFAULT initialization only)
    // -----------------------------------------------------------------------------------
    /** Starts a root {@link BehaviorBuilder} for the given profile name (internal and {@link #example()} use). */
    private static BehaviorBuilder builder(String name) { return new BehaviorBuilderImpl(name, null); }

    /**
     * Deep copy of the built-in root patches, used to restore {@link #DEFAULT} after {@link #restoreBehaviorRegistry()}.
     */
    private static final List<Patch> DEFAULT_PATCH_TEMPLATE;

    /**
     * Root behavior profile (single JVM instance). YAML reloads mutate this object's patch list in place so
     * sessions and other holders keep a stable reference.
     */
    public static final Behavior DEFAULT;

    /**
     * Canonical name of {@link #DEFAULT}. Used only during class initialization where
     * {@code Behavior.DEFAULT.name()} is not yet available; afterward use {@link #name()} on {@link #DEFAULT}.
     */
    private static final String DEFAULT_ROOT_NAME = "DEFAULT";

    static {
        BehaviorBuilderImpl db = (BehaviorBuilderImpl) builder(DEFAULT_ROOT_NAME);
        db.on(Selectors.all(), ops -> ops
                    .abandonCallAfter(Duration.ofSeconds(1))
                    .delayBetweenRetries(Duration.ofMillis(0))
                    .useDurableDelete(false)
                    .maximumNumberOfCallAttempts(3)
                    .maxConcurrentNodes(1)
                    .replicaOrder(Replica.SEQUENCE)  // Old: SEQUENCE, now explicit list
                    .readMode(ReadModeAP.ALL)
                    .consistency(ReadModeSC.SESSION)
                    .resetTtlOnReadAtPercent(0)
                    .sendKey(false)
                    .stackTraceOnException(true)
                    .useCompression(false)
                    .waitForCallToComplete(Duration.ofSeconds(30))
                    .waitForConnectionToComplete(Duration.ofSeconds(0))
                    .waitForSocketResponseAfterCallFails(Duration.ofSeconds(0))
                    .errorDetailVerbosity(ErrorDetailVerbosity.NONE)
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
                    .allowScansWithWhere(false)
                    .maximumNumberOfCallAttempts(6)
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
            // Query write defaults (background operations)
            // Background operations run server-side on entire sets and require different timeout/retry settings
            .on(Selectors.writes().retryable().query(), ops -> ops
                    .maximumNumberOfCallAttempts(1)  // Background tasks don't retry - managed by server
                    .waitForCallToComplete(Duration.ofSeconds(0))  // Don't wait - return immediately with ExecuteTask
                    .abandonCallAfter(Duration.ofSeconds(30))  // Generous timeout for large scan operations
            )
            .on(Selectors.writes().nonRetryable().query(), ops -> ops
                    .maximumNumberOfCallAttempts(1)  // Background tasks don't retry - managed by server
                    .waitForCallToComplete(Duration.ofSeconds(0))  // Don't wait - return immediately with ExecuteTask
                    .abandonCallAfter(Duration.ofSeconds(30))  // Generous timeout for large scan operations
            )
            // AP write defaults
            .on(Selectors.writes().ap(), ops -> ops
                    .commitLevel(CommitLevel.COMMIT_ALL)
            )
            // Transaction - txnVerify defaults
            .on(Selectors.transaction().txnVerify(), ops -> ops
                    .consistency(ReadModeSC.LINEARIZE)
                    .replicaOrder(Replica.MASTER)
                    .maximumNumberOfCallAttempts(6)
                    .waitForCallToComplete(Duration.ofSeconds(3))
                    .abandonCallAfter(Duration.ofSeconds(10))
                    .delayBetweenRetries(Duration.ofSeconds(1))
                    .allowInlineMemoryAccess(false)
                    .allowInlineSsdAccess(true)
                    .sendKey(false)
            )
            // Transaction - txnRoll defaults
            .on(Selectors.transaction().txnRoll(), ops -> ops
                    .replicaOrder(Replica.MASTER)
                    .maximumNumberOfCallAttempts(6)
                    .waitForCallToComplete(Duration.ofSeconds(3))
                    .abandonCallAfter(Duration.ofSeconds(10))
                    .delayBetweenRetries(Duration.ofSeconds(1))
                    .allowInlineMemoryAccess(false)
                    .allowInlineSsdAccess(true)
                    .sendKey(false)
            );
        DEFAULT = new Behavior(DEFAULT_ROOT_NAME, db.patches, null);
        DEFAULT_PATCH_TEMPLATE = List.copyOf(deepCopyPatchList(DEFAULT.patches));
    }

    /**
     * Resets {@link #DEFAULT} to the built-in policy defaults (as shipped), discarding any YAML overlays
     * that were merged into the root profile.
     */
    public static void restoreDefaultRootPatches() {
        DEFAULT.reloadDefaultRootFromTemplateOnly();
    }

    // -----------------------------------------------------------------------------------
    // Behavior representation (patch list + resolved matrix)
    // -----------------------------------------------------------------------------------
    private final String name;
    private final ArrayList<Patch> patches; // in call order; mutated on YAML reload for registry entries
    private final Behavior base;       // defaults (may be null)
    private final List<Behavior> children;
    private volatile Map<OpKey, ResolvedSettings> resolved; // fully-resolved matrix

    /** Returns an independent copy of each patch so builders or reload logic do not share mutable {@link Settings} state. */
    private static ArrayList<Patch> deepCopyPatchList(List<Patch> src) {
        ArrayList<Patch> out = new ArrayList<>(src.size());
        for (Patch p : src) {
            out.add(p.duplicate());
        }
        return out;
    }

    /**
     * Creates a named profile with the supplied patch list and optional parent; registers the parent/child
     * link for inheritance resolution.
     */
    private Behavior(String name, List<Patch> patches, Behavior base) {
        this.name = name;
        this.patches = deepCopyPatchList(patches);
        this.base = base;
        this.resolved = formMatrix();
        this.children = new ArrayList<>();

        if (base != null) {
            base.children.add(this);
        }
    }

    /**
     * Recomputes this profile’s fully resolved policy matrix from the parent chain and local patches, then
     * propagates the same refresh to every descendant behavior.
     */
    public void clearCache() {
        this.resolved = formMatrix();
        // Notify all children
        for (Behavior child : children) {
            child.clearCache();
        }
    }

    /**
     * Signals that this profile’s configuration changed after construction; refreshes resolved settings for
     * this node and its subtree.
     */
    void changed() {
        clearCache();
    }

    /**
     * Replaces the root {@link #DEFAULT} patch list with the built-in template only (no YAML overlay),
     * for example when resetting the registry in tests.
     */
    void reloadDefaultRootFromTemplateOnly() {
        synchronized (patches) {
            patches.clear();
            for (Patch p : DEFAULT_PATCH_TEMPLATE) {
                patches.add(p.duplicate());
            }
        }
        changed();
    }

    /**
     * Merges YAML-driven overrides into the root {@link #DEFAULT} profile: restores factory defaults, then
     * appends patches derived from the given YAML block so file reload updates the shared {@code DEFAULT}
     * instance without breaking existing references.
     */
    void reloadDefaultRootFromYaml(BehaviorYamlConfig.BehaviorConfig config) {
        BehaviorBuilderImpl builder = new BehaviorBuilderImpl(name, null);
        BehaviorYamlLoader.applyBehaviorConfigToBuilder(builder, config);
        ArrayList<Patch> yamlPatches = builder.snapshotPatchesForReload();
        synchronized (patches) {
            patches.clear();
            for (Patch p : DEFAULT_PATCH_TEMPLATE) {
                patches.add(p.duplicate());
            }
            patches.addAll(yamlPatches);
        }
        changed();
    }

    /**
     * Replaces this (non-root) profile’s YAML-derived patches while keeping the same {@link Behavior} object
     * and parent link, so sessions and other code holding this reference pick up new file configuration when
     * the parent in YAML has not changed.
     */
    void reloadDerivedProfileFromYaml(Behavior parent, String profileName, BehaviorYamlConfig.BehaviorConfig config) {
        BehaviorBuilderImpl builder = new BehaviorBuilderImpl(profileName, parent);
        BehaviorYamlLoader.applyBehaviorConfigToBuilder(builder, config);
        ArrayList<Patch> next = builder.snapshotPatchesForReload();
        synchronized (patches) {
            patches.clear();
            patches.addAll(next);
        }
        changed();
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

    /**
     * Materializes the effective {@link ResolvedSettings} for every concrete operation key by inheriting from
     * the parent matrix (if any) and applying this profile’s patches in declaration order (later wins).
     */
    private Map<OpKey, ResolvedSettings> formMatrix() {
        // 1) Start with parent's resolved matrix (if any).
        Map<OpKey, ResolvedSettings> matrix = new HashMap<>();
        if (base != null && base.resolved != null) {
            // deep copy of settings
            for (Map.Entry<OpKey, ResolvedSettings> e : base.resolved.entrySet()) {
                matrix.put(e.getKey(), e.getValue());
            }
        }

        // 2) Apply this behavior's patches in insertion order.
        //    For each concrete key matched by a patch, overwrite only non-null fields.
        List<OpKey> allKeys = listAllKeys();
        for (Patch p : patches) {
            for (OpKey key : allKeys) {
                if (applies(p.spec, key)) {
                    ResolvedSettings resolved = matrix.get(key);

                    if (resolved != null) {
                        resolved = new ResolvedSettings(resolved, p.settings);
                    }
                    else {
                        resolved = new ResolvedSettings(p.settings);
                    }
                    matrix.put(key, resolved);
                }
            }
        }
        return matrix;
    }

    public String name() { return name; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Behavior[name=").append(name);
        if (base != null) {
            sb.append(", parent=").append(base.name);
        }
        sb.append(", patches=").append(patches.size());
        if (!children.isEmpty()) {
            sb.append(", children=").append(children.size());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns merged policy for transaction system operations ({@link OpKind#SYSTEM_TXN_VERIFY} or
     * {@link OpKind#SYSTEM_TXN_ROLL}).
     *
     * @throws IllegalArgumentException if {@code kind} is not a system operation kind
     */
    public ResolvedSettings getSystemSettings(OpKind kind) {
        if (kind.isSystem()) {
            return resolved.get(new OpKey(kind, OpShape.SYSTEM, Mode.ANY));
        }
        else {
            throw new IllegalArgumentException("Only SYSTEM_* OpKinds are supported");
        }
    }
    /**
     * Returns merged client policy for the given operation kind, shape, and AP/CP mode, or {@code null}
     * when the matrix has no row for that key.
     */
    public ResolvedSettings getSettings(OpKind kind, OpShape shape, Mode mode) {
        if (kind.isSystem()) {
            return getSystemSettings(kind);
        }
        return resolved.get(new OpKey(kind, shape, mode));
    }

    /**
     * Returns merged client policy for the given operation dimensions, choosing AP vs CP mode from whether
     * the target namespace is strong-consistency, or {@code null} when the matrix has no row for that key.
     */
    public ResolvedSettings getSettings(OpKind kind, OpShape shape, boolean isNamespaceSC) {
        if (kind.isSystem()) {
            return getSystemSettings(kind);
        }
        return resolved.get(new OpKey(kind, shape, isNamespaceSC ? Mode.CP : Mode.AP));
    }

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
     * @apiNote The returned profile is registered under {@code name} so it can be resolved via
     * {@link #getBehavior(String)} and updated from YAML like other named profiles.
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
     * Searches the subtree rooted at this behavior (including descendants) for a profile with the given name.
     */
    public Optional<Behavior> findBehavior(String name) {
        return BehaviorRegistry.getInstance().findInTree(this, name);
    }

    /**
     * Returns the named profile from the global registry, or {@link #DEFAULT} when no such profile exists.
     */
    public static Behavior getBehavior(String name) {
        return BehaviorRegistry.getInstance().getBehaviorOrDefault(name);
    }

    /**
     * Returns every behavior currently registered by name (including {@link #DEFAULT} and any derived
     * profiles loaded from YAML or created through {@link #deriveWithChanges}).
     */
    public static Set<Behavior> getAllBehaviors() {
        return BehaviorRegistry.getInstance().getAllBehaviors().entrySet().stream()
            .map(entry -> entry.getValue())
            .collect(Collectors.toSet());
    }

    /**
     * Drops every custom-registered profile and restores {@link #DEFAULT} to its built-in patch set, leaving
     * only the root profile in the registry.
     */
    public static void restoreBehaviorRegistry() {
        BehaviorRegistry.getInstance().clear();
    }

    /**
     * Watches the given behavior YAML file and reapplies its contents whenever the file changes on disk.
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
     * Stops the YAML file watcher started by {@link #startMonitoring(String)} (or overloads) and releases
     * its watch service resources.
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
     * Re-reads the monitored YAML file immediately and applies behavior (and related) updates, without waiting
     * for the file watcher delay.
     */
    public static void reloadBehaviors() {
        BehaviorFileMonitor.getInstance().reloadBehaviors();
    }

    /**
     * Shuts down the background executor used for YAML monitoring and reload scheduling.
     */
    public static void shutdownMonitor() {
        BehaviorFileMonitor.getInstance().shutdown();
    }

    /** Produces a human-readable dump of this profile’s patch stack and resolved matrix for diagnostics. */
    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("Behavior: ").append(name).append('\n');

        sb.append("--- Patches ---").append('\n');
        if (patches.isEmpty()) {
            sb.append("(no overrides)").append('\n');
        }
        int i = 0;
        for (Patch p : patches) {
            sb.append(String.format(Locale.ROOT, "%02d %s -> %s", ++i, p.spec, p.settings)).append('\n');
        }

        sb.append("--- Resolved Matrix ---").append('\n');
        for (OpKey k : listAllKeys()) {
            ResolvedSettings s = resolved.get(k);
            if (s != null) {
                sb.append(k).append(" => ").append(s).append('\n');
            }
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------------------
    /**
     * Builder for constructing Behavior instances with configuration.
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
         * Returns a new {@link Behavior} that captures the configured patches and parent link.
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
            if (!(selector instanceof TypedSelector<?>)) {
                throw new IllegalArgumentException("Selector must be created by Behavior.Selectors");
            }

            Patch patch = new Patch(selector.spec());
            patches.add(patch);
            @SuppressWarnings("unchecked")
            TypedSelector<T> typedSelector = (TypedSelector<T>) selector;
            return TweaksProxy.create(typedSelector.tweaksType(), patch);
        }

        @Override public <T extends TweaksView> BehaviorBuilder on(Selector<T> selector, java.util.function.Consumer<T> apply) {
            T tweaks = on(selector);
            apply.accept(tweaks);
            return this;
        }

        @Override public Behavior build() {
            return new Behavior(name, patches, base);
        }

        /**
         * Produces a detached deep copy of the patches accumulated on this builder for YAML reload handling.
         */
        ArrayList<Patch> snapshotPatchesForReload() {
            ArrayList<Patch> out = new ArrayList<>(patches.size());
            for (Patch p : patches) {
                out.add(p.duplicate());
            }
            return out;
        }
    }

    // -----------------------------------------------------------------------------------
    // Dimensions
    // -----------------------------------------------------------------------------------
    public enum OpKind {
        READ(false),
        WRITE_RETRYABLE(false),
        WRITE_NON_RETRYABLE(false),
        SYSTEM_TXN_VERIFY(true),
        SYSTEM_TXN_ROLL(true);

        private boolean system;
        private OpKind(boolean isSystem) {
            this.system = isSystem;
        }

        public boolean isSystem() {
            return system;
        }
    }

    public enum OpShape { ANY, POINT, BATCH, QUERY, SYSTEM }
    public enum Mode { ANY, AP, CP }

    // -----------------------------------------------------------------------------------
    // Selection spec + resolution helpers
    // -----------------------------------------------------------------------------------
    static final class SelectionSpec {
        final OpKind kind;   // null == ALL kinds (all()) OR both write kinds (writes())
        final OpShape shape; // ANY/POINT/BATCH/QUERY/SYSTEM
        final Mode mode;     // ANY/AP/CP
        final boolean isWriteOnlyWildcard; // true if kind==null means "both write kinds only"

        SelectionSpec(OpKind kind, OpShape shape, Mode mode) {
            this(kind, shape, mode, false);
        }

        SelectionSpec(OpKind kind, OpShape shape, Mode mode, boolean isWriteOnlyWildcard) {
            this.kind = kind;
            this.shape = shape;
            this.mode = mode;
            this.isWriteOnlyWildcard = isWriteOnlyWildcard;
        }

        SelectionSpec withKind(OpKind k)  { return new SelectionSpec(k, shape, mode, false); }
        SelectionSpec withShape(OpShape s){ return new SelectionSpec(kind, s, mode, isWriteOnlyWildcard); }
        SelectionSpec withMode(Mode m)    { return new SelectionSpec(kind, shape, m, isWriteOnlyWildcard); }

        @Override public String toString() {
            String kindStr = kind == null ? (isWriteOnlyWildcard ? "WRITES" : "ALL") : kind.toString();
            return "[" + kindStr + ", " + shape + ", " + mode + "]";
        }
    }

    /** Resolution key (concrete kind, shape, mode). */
    static final class OpKey {
        final OpKind kind; final OpShape shape; final Mode mode;
        OpKey(OpKind k, OpShape s, Mode m) { this.kind = k; this.shape = s; this.mode = m; }
        @Override public boolean equals(Object o) {
            if (!(o instanceof OpKey)) {
                return false;
            }
            OpKey x = (OpKey)o; return kind==x.kind && shape==x.shape && mode==x.mode;
        }
        @Override public int hashCode() { return Objects.hash(kind, shape, mode); }
        @Override public String toString(){ return kind + ":" + shape + ":" + mode; }
    }

    /**
     * Reports whether a concrete operation key lies within the selector scope described by {@code s}
     * (kind, shape, mode, and write-only wildcard semantics).
     */
    static boolean applies(SelectionSpec s, OpKey k) {
        if (s.kind == null) {
            if (s.isWriteOnlyWildcard) {
                // Only match write operations
                if (k.kind != OpKind.WRITE_RETRYABLE && k.kind != OpKind.WRITE_NON_RETRYABLE) {
                    return false;
                }
            }
            // else: kind==null with isWriteOnlyWildcard==false means match ALL kinds (from all())
        } else if (s.kind != k.kind) {
            return false;
        }

        if (s.shape != OpShape.ANY && s.shape != k.shape) {
            return false;
        }
        if (s.mode  != Mode.ANY    && s.mode  != k.mode ) {
            return false;
        }
        return true;
    }

    /** Enumerates every concrete {@link OpKey} used when flattening selector patches into the resolved matrix. */
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
            out.add(new OpKey(OpKind.WRITE_RETRYABLE, OpShape.QUERY, m));
        }
        // NON-RETRYABLE WRITES
        for (Mode m : Mode.values()) {
            out.add(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.POINT, m));
            out.add(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, m));
            out.add(new OpKey(OpKind.WRITE_NON_RETRYABLE, OpShape.QUERY, m));
        }
        // SYSTEM operations (only ANY mode is meaningful for system operations)
        out.add(new OpKey(OpKind.SYSTEM_TXN_VERIFY, OpShape.SYSTEM, Mode.ANY));
        out.add(new OpKey(OpKind.SYSTEM_TXN_ROLL, OpShape.SYSTEM, Mode.ANY));
        return out;
    }

    // -----------------------------------------------------------------------------------
    // Settings captured by each patch (extend with your SettablePolicy knobs)
    // -----------------------------------------------------------------------------------

    /** One selector-sized overlay on the policy matrix, holding the knob values applied for that scope. */
    static final class Patch {
        final SelectionSpec spec;
        final Settings settings = new Settings();

        Patch(SelectionSpec spec) {
            this.spec = spec;
        }

        /** Returns an independent patch with the same scope and knob values. */
        Patch duplicate() {
            Patch q = new Patch(spec);
            q.settings.assignFrom(this.settings);
            return q;
        }
    }

    // -----------------------------------------------------------------------------------
    // Tweak view model — generic capability interfaces + concrete marker views
    // -----------------------------------------------------------------------------------
    public interface TweaksView {}

    public interface CommonTweaks<T extends CommonTweaks<T>> extends TweaksView {
        T abandonCallAfter(Duration d);
        T delayBetweenRetries(Duration d);
        T maximumNumberOfCallAttempts(int n);
        T replicaOrder(Replica r);
        T sendKey(boolean sendKey);
        T useCompression(boolean compress);
        T waitForCallToComplete(Duration d);
        T waitForConnectionToComplete(Duration d);
        T waitForSocketResponseAfterCallFails(Duration d);
        T errorDetailVerbosity(int e);
        T stackTraceOnException(boolean enabled);
    }

    public interface QueryTweaks<T extends QueryTweaks<T>> extends CommonTweaks<T> {
        T recordQueueSize(int n);
        T allowScansWithWhere(boolean allow);
    }

    public interface BatchTweaks<T extends BatchTweaks<T>> extends CommonTweaks<T> {
        T maxConcurrentNodes(int n);
        T allowInlineMemoryAccess(boolean v);
        T allowInlineSsdAccess(boolean v);
    }

    public interface WriteTweaks<T extends WriteTweaks<T>> extends CommonTweaks<T> {
        T useDurableDelete(boolean b);
        T simulateXdrWrite(boolean b);
    }

    public interface WriteApTweaks<T extends WriteApTweaks<T>> extends WriteTweaks<T> {
        T commitLevel(CommitLevel level);
    }

    public interface ReadTweaks<T extends ReadTweaks<T>> extends CommonTweaks<T> {
        T resetTtlOnReadAtPercent(int percent);
    }

    public interface ReadApTweaks<T extends ReadApTweaks<T>> extends ReadTweaks<T> {
        T readMode(ReadModeAP mode);
    }

    public interface ReadCpTweaks<T extends ReadCpTweaks<T>> extends ReadTweaks<T> {
        T consistency(ReadModeSC c);
    }

    public interface RetryableWriteTweaks<T extends RetryableWriteTweaks<T>> extends WriteTweaks<T> {}
    public interface NonRetryableWriteTweaks<T extends NonRetryableWriteTweaks<T>> extends WriteTweaks<T> {}

    // Concrete marker views compose the capabilities valid for each selector.
    public interface AllAnyModeTweaks extends
        BatchTweaks<AllAnyModeTweaks>,
        ReadApTweaks<AllAnyModeTweaks>,
        ReadCpTweaks<AllAnyModeTweaks>,
        WriteApTweaks<AllAnyModeTweaks> {
        AllAnyModeTweaks recordQueueSize(int n);
    }

    public interface ReadAnyAnyModeTweaks extends ReadTweaks<ReadAnyAnyModeTweaks> {}
    public interface ReadAnyApTweaks extends ReadApTweaks<ReadAnyApTweaks> {}
    public interface ReadAnyCpTweaks extends ReadCpTweaks<ReadAnyCpTweaks> {}

    public interface WriteRootAnyModeTweaks extends WriteTweaks<WriteRootAnyModeTweaks> {}
    public interface WriteRootApTweaks extends WriteApTweaks<WriteRootApTweaks> {}
    public interface WriteRootCpTweaks extends WriteTweaks<WriteRootCpTweaks> {}

    public interface ReadPointAnyModeTweaks extends CommonTweaks<ReadPointAnyModeTweaks> {}
    public interface ReadBatchAnyModeTweaks extends BatchTweaks<ReadBatchAnyModeTweaks> {}
    public interface ReadQueryAnyModeTweaks extends QueryTweaks<ReadQueryAnyModeTweaks> {}
    public interface ReadPointApTweaks extends ReadApTweaks<ReadPointApTweaks> {}
    public interface ReadPointCpTweaks extends ReadCpTweaks<ReadPointCpTweaks> {}
    public interface ReadBatchApTweaks extends
        BatchTweaks<ReadBatchApTweaks>, ReadApTweaks<ReadBatchApTweaks> {}
    public interface ReadBatchCpTweaks extends
        BatchTweaks<ReadBatchCpTweaks>, ReadCpTweaks<ReadBatchCpTweaks> {}
    public interface ReadQueryApTweaks extends
        ReadApTweaks<ReadQueryApTweaks>, QueryTweaks<ReadQueryApTweaks> {}
    public interface ReadQueryCpTweaks extends
        ReadCpTweaks<ReadQueryCpTweaks>, QueryTweaks<ReadQueryCpTweaks> {}

    public interface WritePointAnyModeTweaks extends WriteTweaks<WritePointAnyModeTweaks> {}
    public interface WritePointApTweaks extends WriteApTweaks<WritePointApTweaks> {}
    public interface WritePointCpTweaks extends WriteTweaks<WritePointCpTweaks> {}
    public interface WriteBatchAnyModeTweaks extends
        BatchTweaks<WriteBatchAnyModeTweaks>, WriteTweaks<WriteBatchAnyModeTweaks> {}
    public interface WriteBatchApTweaks extends
        BatchTweaks<WriteBatchApTweaks>, WriteApTweaks<WriteBatchApTweaks> {}
    public interface WriteBatchCpTweaks extends
        BatchTweaks<WriteBatchCpTweaks>, WriteTweaks<WriteBatchCpTweaks> {}

    public interface RetryableWriteAnyModeTweaks extends
        RetryableWriteTweaks<RetryableWriteAnyModeTweaks> {}
    public interface RetryableWritePointAnyModeTweaks extends
        RetryableWriteTweaks<RetryableWritePointAnyModeTweaks> {}
    public interface RetryableWritePointApTweaks extends WriteApTweaks<RetryableWritePointApTweaks> {}
    public interface RetryableWritePointCpTweaks extends
        RetryableWriteTweaks<RetryableWritePointCpTweaks> {}
    public interface RetryableWriteBatchAnyModeTweaks extends
        BatchTweaks<RetryableWriteBatchAnyModeTweaks>,
        RetryableWriteTweaks<RetryableWriteBatchAnyModeTweaks> {}
    public interface RetryableWriteBatchApTweaks extends
        BatchTweaks<RetryableWriteBatchApTweaks>, WriteApTweaks<RetryableWriteBatchApTweaks> {}
    public interface RetryableWriteBatchCpTweaks extends
        BatchTweaks<RetryableWriteBatchCpTweaks>,
        RetryableWriteTweaks<RetryableWriteBatchCpTweaks> {}

    public interface NonRetryableWriteAnyModeTweaks extends
        NonRetryableWriteTweaks<NonRetryableWriteAnyModeTweaks> {}
    public interface NonRetryableWritePointAnyModeTweaks extends
        NonRetryableWriteTweaks<NonRetryableWritePointAnyModeTweaks> {}
    public interface NonRetryableWritePointApTweaks extends WriteApTweaks<NonRetryableWritePointApTweaks> {}
    public interface NonRetryableWritePointCpTweaks extends
        NonRetryableWriteTweaks<NonRetryableWritePointCpTweaks> {}
    public interface NonRetryableWriteBatchAnyModeTweaks extends
        BatchTweaks<NonRetryableWriteBatchAnyModeTweaks>,
        NonRetryableWriteTweaks<NonRetryableWriteBatchAnyModeTweaks> {}
    public interface NonRetryableWriteBatchApTweaks extends
        BatchTweaks<NonRetryableWriteBatchApTweaks>, WriteApTweaks<NonRetryableWriteBatchApTweaks> {}
    public interface NonRetryableWriteBatchCpTweaks extends
        BatchTweaks<NonRetryableWriteBatchCpTweaks>,
        NonRetryableWriteTweaks<NonRetryableWriteBatchCpTweaks> {}

    public interface RetryableWriteQueryAnyModeTweaks extends
        QueryTweaks<RetryableWriteQueryAnyModeTweaks>,
        RetryableWriteTweaks<RetryableWriteQueryAnyModeTweaks> {}
    public interface RetryableWriteQueryApTweaks extends
        QueryTweaks<RetryableWriteQueryApTweaks>, WriteApTweaks<RetryableWriteQueryApTweaks> {}
    public interface RetryableWriteQueryCpTweaks extends
        QueryTweaks<RetryableWriteQueryCpTweaks>,
        RetryableWriteTweaks<RetryableWriteQueryCpTweaks> {}
    public interface NonRetryableWriteQueryAnyModeTweaks extends
        QueryTweaks<NonRetryableWriteQueryAnyModeTweaks>,
        NonRetryableWriteTweaks<NonRetryableWriteQueryAnyModeTweaks> {}
    public interface NonRetryableWriteQueryApTweaks extends
        QueryTweaks<NonRetryableWriteQueryApTweaks>, WriteApTweaks<NonRetryableWriteQueryApTweaks> {}
    public interface NonRetryableWriteQueryCpTweaks extends
        QueryTweaks<NonRetryableWriteQueryCpTweaks>,
        NonRetryableWriteTweaks<NonRetryableWriteQueryCpTweaks> {}

    /**
     * Tweaks for transaction verification operations (read-like settings).
     * Transaction verification is internally implemented as a batch operation.
     */
    public interface SystemTxnVerifyTweaks extends BatchTweaks<SystemTxnVerifyTweaks> {
        SystemTxnVerifyTweaks consistency(ReadModeSC consistency);
    }

    /**
     * Tweaks for transaction rollback operations (write-like settings).
     * Transaction rollback is internally implemented as a batch operation.
     */
    public interface SystemTxnRollTweaks extends BatchTweaks<SystemTxnRollTweaks> {}


    // -----------------------------------------------------------------------------------
    // Selectors + factories
    // -----------------------------------------------------------------------------------
    public interface Selector<T extends TweaksView> {
        SelectionSpec spec();
    }

    private interface TypedSelector<T extends TweaksView> {
        Class<T> tweaksType();
    }

    /**
     * Factory for creating selectors that specify which operations to configure.
     *
     * <p>Selectors use an API to narrow down operation types from general to specific,
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
        public static ReadAnySelector<ReadAnyAnyModeTweaks> reads() { return new ReadAnySel<>(new SelectionSpec(OpKind.READ, OpShape.ANY, Mode.ANY), ReadAnyAnyModeTweaks.class); }

        /**
         * Selects all WRITE operations. Continue chaining to narrow by retryability, shape, or mode.
         *
         * @return selector for write operations
         */
        public static WriteRootSelector<WriteRootAnyModeTweaks> writes() { return new WriteRootSel(new SelectionSpec(null, OpShape.ANY, Mode.ANY, true)); }

        /**
         * Selects transaction-specific operations for verification and rollback.
         *
         * <p><b>Note:</b> System-level settings (connections, circuit breaker, refresh intervals)
         * have been moved to {@link com.aerospike.SystemSettings}. Use
         * {@link com.aerospike.ClusterDefinition#withSystemSettings(com.aerospike.SystemSettings)}
         * to configure those settings.</p>
         *
         * <h3>Sub-categories:</h3>
         * <ul>
         *   <li><b>txnVerify</b> - Transaction verification operations (read-like)</li>
         *   <li><b>txnRoll</b> - Transaction rollback operations (write-like)</li>
         * </ul>
         *
         * <h3>Example usage:</h3>
         * <pre>{@code
         * Behavior custom = Behavior.DEFAULT.deriveWithChanges("custom", builder -> builder
         *     .on(Selectors.transaction().txnVerify(), ops -> ops
         *         .consistency(ReadModeSC.LINEARIZE)
         *         .maximumNumberOfCallAttempts(10)
         *         .waitForCallToComplete(Duration.ofSeconds(5))
         *     )
         *     .on(Selectors.transaction().txnRoll(), ops -> ops
         *         .maximumNumberOfCallAttempts(6)
         *         .delayBetweenRetries(Duration.ofSeconds(1))
         *     )
         * );
         * }</pre>
         *
         * @return selector for transaction operations
         */
        public static TransactionRootSelector transaction() {
            return new TransactionRootSel(new SelectionSpec(null, OpShape.SYSTEM, Mode.ANY));
        }
    }

    public static final class AllSelector implements Selector<AllAnyModeTweaks>, TypedSelector<AllAnyModeTweaks> {
        private final SelectionSpec spec;
        AllSelector(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<AllAnyModeTweaks> tweaksType() { return AllAnyModeTweaks.class; }
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

    static final class ReadAnySel<T extends TweaksView> implements ReadAnySelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        ReadAnySel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }

        @Override public ReadPointSelector<ReadPointAnyModeTweaks> get()  { return new ReadPointSel<>(spec.withShape(OpShape.POINT), ReadPointAnyModeTweaks.class); }
        @Override public ReadBatchSelector<ReadBatchAnyModeTweaks> batch(){ return new ReadBatchSel<>(spec.withShape(OpShape.BATCH), ReadBatchAnyModeTweaks.class); }
        @Override public ReadQuerySelector<ReadQueryAnyModeTweaks> query(){ return new ReadQuerySel<>(spec.withShape(OpShape.QUERY), ReadQueryAnyModeTweaks.class); }

        @Override public ReadAnySelector<ReadAnyApTweaks> ap() { return new ReadAnySel<>(spec.withMode(Mode.AP), ReadAnyApTweaks.class); }
        @Override public ReadAnySelector<ReadAnyCpTweaks> cp() { return new ReadAnySel<>(spec.withMode(Mode.CP), ReadAnyCpTweaks.class); }
    }
    static final class ReadPointSel<T extends TweaksView> implements ReadPointSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        ReadPointSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public ReadPointSelector<ReadPointApTweaks> ap() { return new ReadPointSel<>(spec.withMode(Mode.AP), ReadPointApTweaks.class); }
        @Override public ReadPointSelector<ReadPointCpTweaks> cp() { return new ReadPointSel<>(spec.withMode(Mode.CP), ReadPointCpTweaks.class); }
    }
    static final class ReadBatchSel<T extends TweaksView> implements ReadBatchSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        ReadBatchSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public ReadBatchSelector<ReadBatchApTweaks> ap() { return new ReadBatchSel<>(spec.withMode(Mode.AP), ReadBatchApTweaks.class); }
        @Override public ReadBatchSelector<ReadBatchCpTweaks> cp() { return new ReadBatchSel<>(spec.withMode(Mode.CP), ReadBatchCpTweaks.class); }
    }
    static final class ReadQuerySel<T extends TweaksView> implements ReadQuerySelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        ReadQuerySel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public ReadQuerySelector<ReadQueryApTweaks> ap() { return new ReadQuerySel<>(spec.withMode(Mode.AP), ReadQueryApTweaks.class); }
        @Override public ReadQuerySelector<ReadQueryCpTweaks> cp() { return new ReadQuerySel<>(spec.withMode(Mode.CP), ReadQueryCpTweaks.class); }
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
        RetryableWriteQuerySelector<RetryableWriteQueryAnyModeTweaks> query();
    }
    public interface RetryableWritePointSelector<T extends TweaksView> extends Selector<T> {
        RetryableWritePointSelector<RetryableWritePointApTweaks> ap();
        RetryableWritePointSelector<RetryableWritePointCpTweaks> cp();
    }
    public interface RetryableWriteBatchSelector<T extends TweaksView> extends Selector<T> {
        RetryableWriteBatchSelector<RetryableWriteBatchApTweaks> ap();
        RetryableWriteBatchSelector<RetryableWriteBatchCpTweaks> cp();
    }
    public interface RetryableWriteQuerySelector<T extends TweaksView> extends Selector<T> {
        RetryableWriteQuerySelector<RetryableWriteQueryApTweaks> ap();
        RetryableWriteQuerySelector<RetryableWriteQueryCpTweaks> cp();
    }
    public interface NonRetryableWriteSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWriteSelector<T> ap();
        NonRetryableWriteSelector<T> cp();
        NonRetryableWritePointSelector<NonRetryableWritePointAnyModeTweaks> point();
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchAnyModeTweaks> batch();
        NonRetryableWriteQuerySelector<NonRetryableWriteQueryAnyModeTweaks> query();
    }
    public interface NonRetryableWritePointSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWritePointSelector<NonRetryableWritePointApTweaks> ap();
        NonRetryableWritePointSelector<NonRetryableWritePointCpTweaks> cp();
    }
    public interface NonRetryableWriteBatchSelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchApTweaks> ap();
        NonRetryableWriteBatchSelector<NonRetryableWriteBatchCpTweaks> cp();
    }
    public interface NonRetryableWriteQuerySelector<T extends TweaksView> extends Selector<T> {
        NonRetryableWriteQuerySelector<NonRetryableWriteQueryApTweaks> ap();
        NonRetryableWriteQuerySelector<NonRetryableWriteQueryCpTweaks> cp();
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

    // SYSTEM selectors
    /**
     * Root selector for system-level operations.
     * Allows selection of specific system sub-categories: txnVerify, txnRoll, connections, circuitBreaker, refresh.
     */
    /**
     * Root selector for transaction operations.
     *
     * <p>Provides access to transaction verification and rollback operation selectors.
     * These operations are used internally by the client to manage multi-record transactions.</p>
     *
     * <p><b>Note:</b> System-level settings (connections, circuit breaker, refresh intervals)
     * have been moved to {@link com.aerospike.SystemSettings} and are no longer configurable via Behaviors.
     * Only transaction-specific operations remain in this selector.</p>
     *
     * @see com.aerospike.SystemSettings
     * @see com.aerospike.SystemSettingsRegistry
     */
    public interface TransactionRootSelector {
        SystemTxnVerifySelector txnVerify();
        SystemTxnRollSelector txnRoll();
    }

    /**
     * Selector for transaction verification operations (read-like settings).
     */
    public interface SystemTxnVerifySelector extends Selector<SystemTxnVerifyTweaks> {}

    /**
     * Selector for transaction rollback operations (write-like settings).
     */
    public interface SystemTxnRollSelector extends Selector<SystemTxnRollTweaks> {}


    public static final class WriteRootSel implements WriteRootSelector<WriteRootAnyModeTweaks>, TypedSelector<WriteRootAnyModeTweaks> {
        private final SelectionSpec spec;
        WriteRootSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<WriteRootAnyModeTweaks> tweaksType() { return WriteRootAnyModeTweaks.class; }

        // Mode-first pattern: returns mode-specific selector types (WriteRootApTweaks, WriteRootCpTweaks)
        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return new WriteRootApSel(spec.withMode(Mode.AP)); }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return new WriteRootCpSel(spec.withMode(Mode.CP)); }

        // Retryability selection: returns generic "AnyMode" types
        // Note: If mode was already set via .ap()/.cp(), the SelectionSpec carries it forward,
        // but the return type doesn't reflect mode-specific methods (type system limitation)
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }

        // Shape selection (retryability-agnostic - applies to both retryable and non-retryable)
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT), WritePointAnyModeTweaks.class); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH), WriteBatchAnyModeTweaks.class); }
    }
    public static final class WriteRootApSel implements WriteRootSelector<WriteRootApTweaks>, TypedSelector<WriteRootApTweaks> {
        private final SelectionSpec spec;
        WriteRootApSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<WriteRootApTweaks> tweaksType() { return WriteRootApTweaks.class; }

        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return this; }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return new WriteRootCpSel(spec.withMode(Mode.CP)); }

        // These return generic types, but the AP mode is preserved in the SelectionSpec
        // Limitation: subsequent selector chain won't expose AP-specific methods like commitLevel()
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }

        // Shape selection (retryability-agnostic) - AP mode preserved in SelectionSpec
        // Note: returns generic AnyMode types, but can still call .ap() afterward to expose AP-specific methods
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT), WritePointAnyModeTweaks.class); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH), WriteBatchAnyModeTweaks.class); }
    }
    public static final class WriteRootCpSel implements WriteRootSelector<WriteRootCpTweaks>, TypedSelector<WriteRootCpTweaks> {
        private final SelectionSpec spec;
        WriteRootCpSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<WriteRootCpTweaks> tweaksType() { return WriteRootCpTweaks.class; }

        @Override public WriteRootSelector<WriteRootApTweaks> ap() { return new WriteRootApSel(spec.withMode(Mode.AP)); }
        @Override public WriteRootSelector<WriteRootCpTweaks> cp() { return this; }

        // These return generic types, but the CP mode is preserved in the SelectionSpec
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> retryable() { return new RetryableWriteAnySel(spec.withKind(OpKind.WRITE_RETRYABLE)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> nonRetryable() { return new NonRetryableWriteAnySel(spec.withKind(OpKind.WRITE_NON_RETRYABLE)); }

        // Shape selection (retryability-agnostic) - CP mode preserved in SelectionSpec
        // Note: returns generic AnyMode types, but can still call .cp() afterward to expose CP-specific methods
        @Override public WritePointSelector<WritePointAnyModeTweaks> point() { return new WritePointSel<>(spec.withShape(OpShape.POINT), WritePointAnyModeTweaks.class); }
        @Override public WriteBatchSelector<WriteBatchAnyModeTweaks> batch() { return new WriteBatchSel<>(spec.withShape(OpShape.BATCH), WriteBatchAnyModeTweaks.class); }
    }
    public static final class RetryableWriteAnySel implements RetryableWriteSelector<RetryableWriteAnyModeTweaks>, TypedSelector<RetryableWriteAnyModeTweaks> {
        private final SelectionSpec spec;
        RetryableWriteAnySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<RetryableWriteAnyModeTweaks> tweaksType() { return RetryableWriteAnyModeTweaks.class; }

        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> ap() { return new RetryableWriteAnySel(spec.withMode(Mode.AP)); }
        @Override public RetryableWriteSelector<RetryableWriteAnyModeTweaks> cp() { return new RetryableWriteAnySel(spec.withMode(Mode.CP)); }

        @Override public RetryableWritePointSelector<RetryableWritePointAnyModeTweaks> point() { return new RetryableWritePointSel<>(spec.withShape(OpShape.POINT), RetryableWritePointAnyModeTweaks.class); }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchAnyModeTweaks> batch() { return new RetryableWriteBatchSel<>(spec.withShape(OpShape.BATCH), RetryableWriteBatchAnyModeTweaks.class); }
        @Override public RetryableWriteQuerySelector<RetryableWriteQueryAnyModeTweaks> query() { return new RetryableWriteQuerySel<>(spec.withShape(OpShape.QUERY), RetryableWriteQueryAnyModeTweaks.class); }
    }
    public static final class RetryableWritePointSel<T extends TweaksView> implements RetryableWritePointSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        RetryableWritePointSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public RetryableWritePointSelector<RetryableWritePointApTweaks> ap() { return new RetryableWritePointSel<>(spec.withMode(Mode.AP), RetryableWritePointApTweaks.class); }
        @Override public RetryableWritePointSelector<RetryableWritePointCpTweaks> cp() { return new RetryableWritePointSel<>(spec.withMode(Mode.CP), RetryableWritePointCpTweaks.class); }
    }
    public static final class RetryableWriteBatchSel<T extends TweaksView> implements RetryableWriteBatchSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        RetryableWriteBatchSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchApTweaks> ap() { return new RetryableWriteBatchSel<>(spec.withMode(Mode.AP), RetryableWriteBatchApTweaks.class); }
        @Override public RetryableWriteBatchSelector<RetryableWriteBatchCpTweaks> cp() { return new RetryableWriteBatchSel<>(spec.withMode(Mode.CP), RetryableWriteBatchCpTweaks.class); }
    }
    public static final class RetryableWriteQuerySel<T extends TweaksView> implements RetryableWriteQuerySelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        RetryableWriteQuerySel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public RetryableWriteQuerySelector<RetryableWriteQueryApTweaks> ap() { return new RetryableWriteQuerySel<>(spec.withMode(Mode.AP), RetryableWriteQueryApTweaks.class); }
        @Override public RetryableWriteQuerySelector<RetryableWriteQueryCpTweaks> cp() { return new RetryableWriteQuerySel<>(spec.withMode(Mode.CP), RetryableWriteQueryCpTweaks.class); }
    }
    public static final class NonRetryableWriteAnySel implements NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks>, TypedSelector<NonRetryableWriteAnyModeTweaks> {
        private final SelectionSpec spec;
        NonRetryableWriteAnySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<NonRetryableWriteAnyModeTweaks> tweaksType() { return NonRetryableWriteAnyModeTweaks.class; }

        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> ap() { return new NonRetryableWriteAnySel(spec.withMode(Mode.AP)); }
        @Override public NonRetryableWriteSelector<NonRetryableWriteAnyModeTweaks> cp() { return new NonRetryableWriteAnySel(spec.withMode(Mode.CP)); }

        @Override public NonRetryableWritePointSelector<NonRetryableWritePointAnyModeTweaks> point() { return new NonRetryableWritePointSel<>(spec.withShape(OpShape.POINT), NonRetryableWritePointAnyModeTweaks.class); }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchAnyModeTweaks> batch() { return new NonRetryableWriteBatchSel<>(spec.withShape(OpShape.BATCH), NonRetryableWriteBatchAnyModeTweaks.class); }
        @Override public NonRetryableWriteQuerySelector<NonRetryableWriteQueryAnyModeTweaks> query() { return new NonRetryableWriteQuerySel<>(spec.withShape(OpShape.QUERY), NonRetryableWriteQueryAnyModeTweaks.class); }
    }
    public static final class NonRetryableWritePointSel<T extends TweaksView> implements NonRetryableWritePointSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        NonRetryableWritePointSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public NonRetryableWritePointSelector<NonRetryableWritePointApTweaks> ap() { return new NonRetryableWritePointSel<>(spec.withMode(Mode.AP), NonRetryableWritePointApTweaks.class); }
        @Override public NonRetryableWritePointSelector<NonRetryableWritePointCpTweaks> cp() { return new NonRetryableWritePointSel<>(spec.withMode(Mode.CP), NonRetryableWritePointCpTweaks.class); }
    }
    public static final class NonRetryableWriteBatchSel<T extends TweaksView> implements NonRetryableWriteBatchSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        NonRetryableWriteBatchSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchApTweaks> ap() { return new NonRetryableWriteBatchSel<>(spec.withMode(Mode.AP), NonRetryableWriteBatchApTweaks.class); }
        @Override public NonRetryableWriteBatchSelector<NonRetryableWriteBatchCpTweaks> cp() { return new NonRetryableWriteBatchSel<>(spec.withMode(Mode.CP), NonRetryableWriteBatchCpTweaks.class); }
    }
    public static final class NonRetryableWriteQuerySel<T extends TweaksView> implements NonRetryableWriteQuerySelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        NonRetryableWriteQuerySel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public NonRetryableWriteQuerySelector<NonRetryableWriteQueryApTweaks> ap() { return new NonRetryableWriteQuerySel<>(spec.withMode(Mode.AP), NonRetryableWriteQueryApTweaks.class); }
        @Override public NonRetryableWriteQuerySelector<NonRetryableWriteQueryCpTweaks> cp() { return new NonRetryableWriteQuerySel<>(spec.withMode(Mode.CP), NonRetryableWriteQueryCpTweaks.class); }
    }

    // Write shape selector implementations (retryability-agnostic)
    static final class WritePointSel<T extends TweaksView> implements WritePointSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        WritePointSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public WritePointSelector<WritePointApTweaks> ap() { return new WritePointSel<>(spec.withMode(Mode.AP), WritePointApTweaks.class); }
        @Override public WritePointSelector<WritePointCpTweaks> cp() { return new WritePointSel<>(spec.withMode(Mode.CP), WritePointCpTweaks.class); }
    }
    static final class WriteBatchSel<T extends TweaksView> implements WriteBatchSelector<T>, TypedSelector<T> {
        private final SelectionSpec spec;
        private final Class<T> tweaksType;
        WriteBatchSel(SelectionSpec spec, Class<T> tweaksType) {
            this.spec = spec;
            this.tweaksType = tweaksType;
        }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<T> tweaksType() { return tweaksType; }
        @Override public WriteBatchSelector<WriteBatchApTweaks> ap() { return new WriteBatchSel<>(spec.withMode(Mode.AP), WriteBatchApTweaks.class); }
        @Override public WriteBatchSelector<WriteBatchCpTweaks> cp() { return new WriteBatchSel<>(spec.withMode(Mode.CP), WriteBatchCpTweaks.class); }
    }

    // TRANSACTION selector implementations
    static final class TransactionRootSel implements TransactionRootSelector {
        private final SelectionSpec spec;
        TransactionRootSel(SelectionSpec spec) { this.spec = spec; }

        @Override public SystemTxnVerifySelector txnVerify() {
            return new SystemTxnVerifySel(spec.withKind(OpKind.SYSTEM_TXN_VERIFY));
        }
        @Override public SystemTxnRollSelector txnRoll() {
            return new SystemTxnRollSel(spec.withKind(OpKind.SYSTEM_TXN_ROLL));
        }
    }

    static final class SystemTxnVerifySel implements SystemTxnVerifySelector, TypedSelector<SystemTxnVerifyTweaks> {
        private final SelectionSpec spec;
        SystemTxnVerifySel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<SystemTxnVerifyTweaks> tweaksType() { return SystemTxnVerifyTweaks.class; }
    }

    static final class SystemTxnRollSel implements SystemTxnRollSelector, TypedSelector<SystemTxnRollTweaks> {
        private final SelectionSpec spec;
        SystemTxnRollSel(SelectionSpec spec) { this.spec = spec; }
        @Override public SelectionSpec spec() { return spec; }
        @Override public Class<SystemTxnRollTweaks> tweaksType() { return SystemTxnRollTweaks.class; }
    }

    // -----------------------------------------------------------------------------------
    // Tweaks proxy
    // -----------------------------------------------------------------------------------
    // Each proxy implements exactly one marker view. Java does not allow a single class to
    // implement the same generic capability (for example CommonTweaks) with different self types.
    static final class TweaksProxy implements InvocationHandler {
        private static final Map<String, BiConsumer<Settings, Object>> SETTERS = Map.ofEntries(
            Map.entry("abandonCallAfter", (settings, value) -> settings.abandonCallAfter = (Duration)value),
            Map.entry("delayBetweenRetries", (settings, value) -> settings.delayBetweenRetries = (Duration)value),
            Map.entry("maximumNumberOfCallAttempts",
                (settings, value) -> settings.maximumNumberOfCallAttempts = (Integer)value),
            Map.entry("replicaOrder", (settings, value) -> settings.replicaOrder = (Replica)value),
            Map.entry("sendKey", (settings, value) -> settings.sendKey = (Boolean)value),
            Map.entry("useCompression", (settings, value) -> settings.useCompression = (Boolean)value),
            Map.entry("waitForCallToComplete", (settings, value) -> settings.waitForCallToComplete = (Duration)value),
            Map.entry("waitForConnectionToComplete",
                (settings, value) -> settings.waitForConnectionToComplete = (Duration)value),
            Map.entry("waitForSocketResponseAfterCallFails",
                (settings, value) -> settings.waitForSocketResponseAfterCallFails = (Duration)value),
            Map.entry("errorDetailVerbosity", (settings, value) -> settings.errorDetailVerbosity = (Integer)value),
            Map.entry("stackTraceOnException", (settings, value) -> settings.stackTraceOnException = (Boolean)value),
            Map.entry("recordQueueSize", (settings, value) -> settings.recordQueueSize = (Integer)value),
            Map.entry("allowScansWithWhere", (settings, value) -> settings.allowScansWithWhere = (Boolean)value),
            Map.entry("maxConcurrentNodes", (settings, value) -> settings.maxConcurrentNodes = (Integer)value),
            Map.entry("allowInlineMemoryAccess",
                (settings, value) -> settings.allowInlineMemoryAccess = (Boolean)value),
            Map.entry("allowInlineSsdAccess", (settings, value) -> settings.allowInlineSsdAccess = (Boolean)value),
            Map.entry("useDurableDelete", (settings, value) -> settings.useDurableDelete = (Boolean)value),
            Map.entry("simulateXdrWrite", (settings, value) -> settings.simulateXdrWrite = (Boolean)value),
            Map.entry("commitLevel", (settings, value) -> settings.commitLevel = (CommitLevel)value),
            Map.entry("resetTtlOnReadAtPercent",
                (settings, value) -> settings.resetTtlOnReadAtPercent = (Integer)value),
            Map.entry("readMode", (settings, value) -> settings.readModeAP = (ReadModeAP)value),
            Map.entry("consistency", (settings, value) -> settings.readModeSC = (ReadModeSC)value)
        );

        static {
            Set<String> tweakMethods = List.of(
                    CommonTweaks.class,
                    QueryTweaks.class,
                    BatchTweaks.class,
                    WriteTweaks.class,
                    WriteApTweaks.class,
                    ReadTweaks.class,
                    ReadApTweaks.class,
                    ReadCpTweaks.class
                ).stream()
                .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
                .map(Method::getName)
                .collect(Collectors.toSet());

            if (!SETTERS.keySet().equals(tweakMethods)) {
                throw new ExceptionInInitializerError(
                    "Tweak methods and settings handlers differ: methods=" + tweakMethods +
                    ", handlers=" + SETTERS.keySet()
                );
            }
        }

        private final Patch patch;

        private TweaksProxy(Patch patch) {
            this.patch = patch;
        }

        static <T extends TweaksView> T create(Class<T> tweaksType, Patch patch) {
            Object proxy = Proxy.newProxyInstance(
                tweaksType.getClassLoader(),
                new Class<?>[] { tweaksType },
                new TweaksProxy(patch)
            );
            return tweaksType.cast(proxy);
        }

        @Override public Object invoke(Object proxy, Method method, Object[] args) {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "toString" -> "TweaksProxy[" + patch.spec + "]";
                    default -> throw new UnsupportedOperationException(method.toString());
                };
            }

            BiConsumer<Settings, Object> setter = SETTERS.get(method.getName());
            if (setter == null) {
                throw new UnsupportedOperationException(method.toString());
            }
            setter.accept(patch.settings, args[0]);
            return proxy;
        }
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
}
