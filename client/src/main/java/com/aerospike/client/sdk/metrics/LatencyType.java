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
package com.aerospike.client.sdk.metrics;

/**
 * Latency group type.
 */
public enum LatencyType {
    NONE("none", -1),
    CONN("conn", -1),
    WRITE("write", 0),
    READ("read", 1),
    BATCH("batch", 2),
    QUERY("query", 3);

    private String label;
    private int commandOrdinal;

    private LatencyType(String label, int commandOrdinal) {
        this.label = label;
        this.commandOrdinal = commandOrdinal;
    }

    public String getLabel() {
        return label;
    }

    public int getCommandOrdinal() {
        return commandOrdinal;
    }

    public static final LatencyType[] commandTypes = new LatencyType[] {
        LatencyType.WRITE,
        LatencyType.READ,
        LatencyType.BATCH,
        LatencyType.QUERY
    };
}
