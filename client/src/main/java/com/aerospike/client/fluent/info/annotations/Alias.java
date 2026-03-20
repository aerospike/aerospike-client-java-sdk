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
package com.aerospike.client.fluent.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares legacy aliases for an enum constant. When the server returns a value that no longer
 * matches the current enum constant name, alias values are checked as a fallback.
 * <p>
 * This supports backwards compatibility when the Aerospike server renames a value across versions.
 * For example, if {@code indextype} used to return {@code "numeric"} but now returns {@code "integer"},
 * the alias preserves compatibility with older servers:
 * <pre>
 *    {@literal @}Alias({"numeric"})
 *    INTEGER
 * </pre>
 * Alias matching is case-insensitive and applies the same hyphen-to-underscore normalization
 * used for standard enum conversion.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Alias {
    String[] value();
}
