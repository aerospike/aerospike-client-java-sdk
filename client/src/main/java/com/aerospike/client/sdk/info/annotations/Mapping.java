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
package com.aerospike.client.sdk.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Some of the config/metrics are poorly named. For example, there is a "storage-engine" config which
 * can be "memory" or "device", but then there are configs like "storage-engine.defrag-lwm-pct" 
 * which implies that storage-engine is an object in its own right. We cannot change these definitions,
 * but rather we intercept the metrics and rename them to what makes the most sense for parsing them.
 * <p/>
 * In this example, we would use {@code @Mapping(from = "storage-engine", to = "storage-engine.type")} to
 * allow the type to be populated on the storage engine object.
 * <p/>
 * Note that the {@code from} parameter can be a regular expression, and any placeholders matched
 * in this expression can be used by ordinal number in the {@code to} expression.
 * <p/>
 * For example: {@code @Mapping(from = "storage-engine.file\\[(\\d+)\\]", to = "storage-engine.files[$1].filePath")}
 * <p/>
 * Note that mappings can be used directly on a class, or if you need multiple mappings, use the {@link Mappings} class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Mapping {
    String from();
    String to();
}
