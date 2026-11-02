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

/**
 * Programmatic builder for AEL where clauses and expression bins, starting from {@link Ael}.
 *
 * <p><b>Deprecated.</b> The programmatic AEL builder will be removed in the next major release.
 * Use a string AEL where clause, {@code PreparedAel}, or {@code Exp} instead. This builder
 * predates server-side AEL support and covers less than the alternatives. It does not support
 * server-side index selection or the CDT surface.</p>
 *
 * <p>Java has no way to formally deprecate a package: {@code @Deprecated} has no effect on a
 * package declaration (JLS 9.6.4.6), and javadoc does not accept {@code @deprecated} in package
 * documentation. Every type in this package is deprecated individually instead.</p>
 */
package com.aerospike.client.sdk.ael;
