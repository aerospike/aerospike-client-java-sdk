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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.policy.Behavior.Mode;
import com.aerospike.client.sdk.policy.Behavior.OpKind;
import com.aerospike.client.sdk.policy.Behavior.OpShape;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * The allowSingleNodeBatchReads setting: defaults, layering, YAML and reload.
 */
public class AllowSingleNodeBatchReadsSettingTest {

    @AfterEach
    void resetRegistry() {
        Behavior.restoreBehaviorRegistry();
    }

    @Test
    void defaultIsOff() {
        for (Mode mode : new Mode[] {Mode.AP, Mode.CP}) {
            assertFalse(batchRead(Behavior.DEFAULT, mode).getAllowSingleNodeBatchReads(), "batch read " + mode);
            assertFalse(Behavior.DEFAULT.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, mode)
                .getAllowSingleNodeBatchReads(), "retryable batch write " + mode);
            assertFalse(Behavior.DEFAULT.getSettings(OpKind.WRITE_NON_RETRYABLE, OpShape.BATCH, mode)
                .getAllowSingleNodeBatchReads(), "non-retryable batch write " + mode);
        }
        assertFalse(Behavior.DEFAULT.getSystemSettings(OpKind.SYSTEM_TXN_VERIFY).getAllowSingleNodeBatchReads());
        assertFalse(Behavior.DEFAULT.getSystemSettings(OpKind.SYSTEM_TXN_ROLL).getAllowSingleNodeBatchReads());
    }

    @Test
    void batchReadSelectorEnablesOnlyBatchReads() {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("single-node", builder -> builder
            .on(Selectors.reads().batch(), ops -> ops.allowSingleNodeBatchReads(true)));

        assertTrue(batchRead(behavior, Mode.AP).getAllowSingleNodeBatchReads());
        assertTrue(batchRead(behavior, Mode.CP).getAllowSingleNodeBatchReads());
        assertFalse(behavior.getSettings(OpKind.READ, OpShape.POINT, Mode.AP).getAllowSingleNodeBatchReads());
        assertFalse(behavior.getSettings(OpKind.WRITE_RETRYABLE, OpShape.BATCH, Mode.AP)
            .getAllowSingleNodeBatchReads());
    }

    @Test
    void childInheritsWhenUnsetAndOverridesWhenSet() {
        Behavior parent = Behavior.DEFAULT.deriveWithChanges("parent", builder -> builder
            .on(Selectors.reads().batch(), ops -> ops.allowSingleNodeBatchReads(true)));

        Behavior inherits = parent.deriveWithChanges("inherits", builder -> builder
            .on(Selectors.reads().batch(), ops -> ops.maxConcurrentNodes(4)));
        assertTrue(batchRead(inherits, Mode.AP).getAllowSingleNodeBatchReads(), "null inherits from parent");
        assertEquals(4, batchRead(inherits, Mode.AP).getMaxConcurrentNodes());

        Behavior overrides = parent.deriveWithChanges("overrides", builder -> builder
            .on(Selectors.reads().batch(), ops -> ops.allowSingleNodeBatchReads(false)));
        assertFalse(batchRead(overrides, Mode.AP).getAllowSingleNodeBatchReads(), "child overrides parent");
        assertTrue(batchRead(parent, Mode.AP).getAllowSingleNodeBatchReads(), "parent unchanged");
    }

    @Test
    void copyKeepsSettingIncludingNull() {
        Settings enabled = new Settings();
        enabled.allowSingleNodeBatchReads = true;
        assertEquals(Boolean.TRUE, new Settings(enabled).allowSingleNodeBatchReads);

        Settings disabled = new Settings();
        disabled.allowSingleNodeBatchReads = false;
        assertEquals(Boolean.FALSE, new Settings(disabled).allowSingleNodeBatchReads);

        assertNull(new Settings(new Settings()).allowSingleNodeBatchReads, "unset stays unset so it can inherit");
    }

    @Test
    void yamlRoundTripsAndAppearsInSettingsMap() throws IOException {
        Map<String,Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              single-node:
                batch_reads:
                  allow_single_node_batch_reads: true
              split:
                batch_reads:
                  allow_single_node_batch_reads: false
            """);

        Behavior singleNode = behaviors.get("single-node");
        ResolvedSettings on = batchRead(singleNode, Mode.AP);
        assertTrue(on.getAllowSingleNodeBatchReads());
        assertTrue(on.toString().contains("allowSingleNodeBatchReads=true"), on.toString());

        // explain() prints each patch's Settings map, which only lists values that are set.
        String explain = singleNode.explain();
        assertTrue(explain.contains("allowSingleNodeBatchReads=true"), explain);

        ResolvedSettings off = batchRead(behaviors.get("split"), Mode.AP);
        assertFalse(off.getAllowSingleNodeBatchReads());
        assertTrue(off.toString().contains("allowSingleNodeBatchReads=false"), off.toString());
    }

    @Test
    void yamlChildInheritsFromParent() throws IOException {
        String yaml = """
            behaviors:
              parent-batch:
                batch_reads:
                  allow_single_node_batch_reads: true
              child-batch:
                parent: parent-batch
                batch_reads:
                  max_concurrent_servers: 4
            """;

        // Register the parent first so the child's parent reference resolves.
        Map<String,Behavior> behaviors = BehaviorYamlLoader.loadBehaviorsFromString(yaml);
        BehaviorRegistry.getInstance().registerBehavior(behaviors.get("parent-batch"));
        behaviors = BehaviorYamlLoader.loadBehaviorsFromString(yaml);

        ResolvedSettings child = batchRead(behaviors.get("child-batch"), Mode.AP);
        assertTrue(child.getAllowSingleNodeBatchReads());
        assertEquals(4, child.getMaxConcurrentNodes());
    }

    @Test
    void reloadPreservesSetting() throws IOException {
        BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              DEFAULT:
                batch_reads:
                  allow_single_node_batch_reads: true
            """);
        assertTrue(batchRead(Behavior.DEFAULT, Mode.AP).getAllowSingleNodeBatchReads());

        Behavior child = Behavior.DEFAULT.deriveWithChanges("reload-child", builder -> builder
            .on(Selectors.reads().get(), ops -> ops.maximumNumberOfCallAttempts(2)));
        assertTrue(batchRead(child, Mode.AP).getAllowSingleNodeBatchReads());

        // Reload with an unrelated change: the setting survives the patch copies.
        BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              DEFAULT:
                batch_reads:
                  allow_single_node_batch_reads: true
                  max_concurrent_servers: 6
            """);
        assertTrue(batchRead(Behavior.DEFAULT, Mode.AP).getAllowSingleNodeBatchReads());
        assertEquals(6, batchRead(Behavior.DEFAULT, Mode.AP).getMaxConcurrentNodes());
        assertTrue(batchRead(child, Mode.AP).getAllowSingleNodeBatchReads(), "child follows reload");

        // Reload turning it off reaches the child too.
        BehaviorYamlLoader.loadBehaviorsFromString("""
            behaviors:
              DEFAULT:
                batch_reads:
                  allow_single_node_batch_reads: false
            """);
        assertFalse(batchRead(Behavior.DEFAULT, Mode.AP).getAllowSingleNodeBatchReads());
        assertFalse(batchRead(child, Mode.AP).getAllowSingleNodeBatchReads());
    }

    private static ResolvedSettings batchRead(Behavior behavior, Mode mode) {
        return behavior.getSettings(OpKind.READ, OpShape.BATCH, mode);
    }
}
