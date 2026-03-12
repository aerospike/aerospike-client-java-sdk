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
package com.aerospike.dsl;

import java.util.Collection;

/**
 * This class stores namespace and indexes required to build secondary index Filter.
 *
 * @param namespace Namespace to be used for creating secondary index Filter. Is matched with namespace of indexes
 * @param indexes   Collection of {@link Index} objects to be used for creating secondary index Filter.
 *                  Namespace of indexes is matched with the given {@link #namespace}, bin name and index type are matched
 *                  with bins in DSL String
 */
public record IndexContext(String namespace, Collection<Index> indexes) {

    public static IndexContext of(String namespace, Collection<Index> indexes) {
        return new IndexContext(namespace, indexes);
    }
}
