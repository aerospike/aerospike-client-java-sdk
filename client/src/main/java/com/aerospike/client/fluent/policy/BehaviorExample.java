/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.fluent.policy;

import java.time.Duration;

import com.aerospike.client.fluent.policy.Behavior.Selectors;

/**
 * Example demonstrating the new Selector-based Behavior pattern.
 */
public class BehaviorExample {
    
    public static void main(String[] args) {
        // Create a custom behavior using the new Selector pattern
        Behavior newBehavior = Behavior.DEFAULT.deriveWithChanges("newBehavior", builder -> builder
            // Global settings for all operations
            .on(Selectors.all(), ops -> ops
                .waitForSocketResponseAfterCallFails(Duration.ofSeconds(3))
                .maximumNumberOfCallAttempts(3)
            )
            // Settings for all AP-mode reads
            .on(Selectors.reads().ap(), ops -> ops
                .waitForCallToComplete(Duration.ofMillis(25))
                .abandonCallAfter(Duration.ofMillis(100))
            )
            // Settings for batch reads
            .on(Selectors.reads().batch(), ops -> ops
                .maxConcurrentNodes(7)
                .allowInlineMemoryAccess(true)
            )
            // Settings for all writes in CP mode
            .on(Selectors.writes().cp(), ops -> ops
                .useDurableDelete(true)
            )
        );
        
        // Create a child behavior that inherits from the parent
        Behavior childBehavior = newBehavior.deriveWithChanges("child", builder -> builder
            // Override batch write settings
            .on(Selectors.writes().batch(), ops -> ops
                .allowInlineSsdAccess(true)
                .maxConcurrentNodes(5)
            )
            // Override AP read settings
            .on(Selectors.reads().ap(), ops -> ops
                .maximumNumberOfCallAttempts(8)
            )
        );
        
        System.out.println("Created behavior: " + newBehavior.getName());
        System.out.println("Created child behavior: " + childBehavior.getName());
        
        // Get policies using the new dimensional system
        System.out.println("\nGetting settings for READ operations:");
        Settings readSettings = newBehavior.getSettings(
            Behavior.OpKind.READ, 
            Behavior.OpShape.BATCH, 
            Behavior.Mode.AP
        );
        if (readSettings != null) {
            System.out.println("  Max concurrent nodes: " + readSettings.maxConcurrentNodes);
            System.out.println("  Allow inline memory: " + readSettings.allowInlineMemoryAccess);
        }
        
        // Backward compatibility: Get policies using old CommandType
        System.out.println("\nBackward compatibility - using CommandType:");
        WritePolicy writePolicy = newBehavior.getSharedPolicy(Behavior.CommandType.WRITE_RETRYABLE);
        System.out.println("  Total timeout: " + writePolicy.totalTimeout);
        System.out.println("  Max retries: " + writePolicy.maxRetries);
        
        BatchPolicy batchPolicy = childBehavior.getSharedPolicy(Behavior.CommandType.BATCH_READ);
        System.out.println("  Batch max concurrent threads: " + batchPolicy.maxConcurrentThreads);
        System.out.println("  Allow inline: " + batchPolicy.allowInline);
    }
}

