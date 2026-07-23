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

/**
 * Log category string IDs.
 */
public final class Loggers {
    /**
     * AEL language parser.
     */
    public static final String AEL = "ael";

    /**
     * YAML / BehaviorRegistry and client option resolution.
     */
    public static final String BEHAVIOR = "behavior";

    /**
     * Connections.
     */
    public static final String CONNECTION = "connection";

    /**
     * Commands.
     */
    public static final String COMMAND = "command";

    /**
     * Info Commands.
     */
    public static final String INFO = "info";

    /**
     * Metrics.
     */
    public static final String METRICS = "metrics";

    /**
     * All high-volume encode/decode diagnostics.
     */
    public static final String SERDE = "serde";

    /**
     * Cluster tend.
     */
    public static final String TEND = "tend";
}
