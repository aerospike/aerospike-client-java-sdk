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
package com.aerospike.client.fluent;

/**
 * The TypeSafeDataSet allows object mapping functionality without warnings.
 * eg 
 *     session.insert(customerDataSet).object(sampleCust).execute();
 * gives an usafe type compiler warning if just using a DataSet.
 * 
 * TODO: See if this class can be removed and still eliminate the warnings.
 * @param <T>
 */
public class TypeSafeDataSet<T> extends DataSet {
    private final Class<T> clazz;
    public TypeSafeDataSet(String namespace, String set, Class<T> clazz) {
        super(namespace, set);
        this.clazz = clazz;
    }

    public static <R> TypeSafeDataSet<R> of(String namespace, String set, Class<R> clazz) {
        return new TypeSafeDataSet<>(namespace, set, clazz);
    }
    
    public Class<T> getClazz() {
        return clazz;
    }
}
