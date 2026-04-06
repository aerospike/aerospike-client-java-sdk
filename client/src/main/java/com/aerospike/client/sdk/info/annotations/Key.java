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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This field is a key field for merging records together. If two objects have identical key fields, they 
 * will be interpreted as the same object and have the values merged together. When there are multiple nodes in the
 * cluster, they will all report metrics for the same entity, and these need to be merged together.
 * <p/>
 * For example:
 * <ul>
 * <li>Set details must have the name {@code ns} and {@code set} fields to be the same</li>
 * <li>Sindex details must have the name {@code ns}, {@code set}, {@code bin} and {@code indexname} fields to be the same</li>
 * </ul>
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface Key {

}
