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

import com.aerospike.client.sdk.cdt.MapReturnType;

/**
 * Intermediate builder returned by {@link CdtGetOrRemoveBuilder#removeAnd()} and
 * {@link CdtGetOrRemoveBuilder#removeAllOthersAnd()} that allows the caller to specify
 * what data the server should return about the removed elements.
 * <p>
 * Usage:
 * <pre>{@code
 * // Remove entries with values in [1,4) and return their values
 * session.upsert(key)
 *     .bin("map").onMapValueRange(1, 4).removeAnd().getValues()
 *     .execute();
 *
 * // Remove all entries NOT in [1,4) and return the count of removed entries
 * session.upsert(key)
 *     .bin("map").onMapValueRange(1, 4).removeAllOthersAnd().count()
 *     .execute();
 * }</pre>
 *
 * @param <T> the concrete parent operation builder type for chaining
 * @see CdtGetOrRemoveBuilder#removeAnd()
 * @see CdtGetOrRemoveBuilder#removeAllOthersAnd()
 */
public class RemoveResultBuilder<T extends AbstractOperationBuilder<T>> {

    private final CdtGetOrRemoveBuilder<T> cdtBuilder;
    private final int baseFlags;

    RemoveResultBuilder(CdtGetOrRemoveBuilder<T> cdtBuilder, int baseFlags) {
        this.cdtBuilder = cdtBuilder;
        this.baseFlags = baseFlags;
    }

    /**
     * Remove and return the values of the removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T getValues() {
        return cdtBuilder.dispatchRemove(MapReturnType.VALUE | baseFlags);
    }

    /**
     * Remove and return the keys of the removed map entries. Only valid for map operations.
     *
     * @return the parent operation builder for chaining
     * @throws IllegalArgumentException if the current selection is a list operation
     */
    public T getKeys() {
        cdtBuilder.validateMapOnly("removeAnd().getKeys");
        return cdtBuilder.dispatchRemove(MapReturnType.KEY | baseFlags);
    }

    /**
     * Remove and return the count of removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T count() {
        return cdtBuilder.dispatchRemove(MapReturnType.COUNT | baseFlags);
    }

    /**
     * Remove and return the indexes of the removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T getIndexes() {
        return cdtBuilder.dispatchRemove(MapReturnType.INDEX | baseFlags);
    }

    /**
     * Remove and return the reverse indexes of the removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseIndexes() {
        return cdtBuilder.dispatchRemove(MapReturnType.REVERSE_INDEX | baseFlags);
    }

    /**
     * Remove and return the ranks of the removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T getRanks() {
        return cdtBuilder.dispatchRemove(MapReturnType.RANK | baseFlags);
    }

    /**
     * Remove and return the reverse ranks of the removed elements.
     *
     * @return the parent operation builder for chaining
     */
    public T getReverseRanks() {
        return cdtBuilder.dispatchRemove(MapReturnType.REVERSE_RANK | baseFlags);
    }

    /**
     * Remove and return the key-value pairs of the removed map entries. Only valid for map operations.
     *
     * @return the parent operation builder for chaining
     * @throws IllegalArgumentException if the current selection is a list operation
     */
    public T getKeysAndValues() {
        cdtBuilder.validateMapOnly("removeAnd().getKeysAndValues");
        return cdtBuilder.dispatchRemove(MapReturnType.KEY_VALUE | baseFlags);
    }
}
