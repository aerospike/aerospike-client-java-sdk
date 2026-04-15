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

import java.util.List;

public enum NodeCategory {
    MASTER,
    MASTER_OR_REPLICA,
    MASTER_OR_REPLICA_IN_RACK,
    ANY_REPLICA,
    REPLICA_IN_RACK,
    RANDOM,
    RANDOM_IN_RACK;

    public static List<NodeCategory> SEQUENCE = List.of(NodeCategory.MASTER, NodeCategory.ANY_REPLICA);
    public static List<NodeCategory> ALLOW_RACK = List.of(NodeCategory.MASTER_OR_REPLICA_IN_RACK);

}