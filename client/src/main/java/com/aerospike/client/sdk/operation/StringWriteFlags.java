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
package com.aerospike.client.sdk.operation;

/**
 * String operation policy write bit flags. Use BITWISE OR to combine flags. Example:
 *
 * <pre>{@code
 * int flags = StringWriteFlags.UPDATE_ONLY | StringWriteFlags.NO_FAIL;
 * }</pre>
 */
public final class StringWriteFlags {
	/**
	 * Default. Allow create or update.
	 */
	public static final int DEFAULT = 0;

	/**
	 * Apply only if the string bin does not already exist.
	 * <p>
	 * Mutually exclusive with {@link #UPDATE_ONLY}. Valid only for create-capable
	 * string modify operations: insert, overwrite, concat, append, prepend,
	 * padStart, padEnd, and repeat. Invalid when operating through a CDT
	 * {@code CTX} path.
	 */
	public static final int CREATE_ONLY = 1;

	/**
	 * Apply only if the string bin already exists. If the bin is missing, the
	 * operation is a no-op and does not create the bin.
	 * <p>
	 * Mutually exclusive with {@link #CREATE_ONLY}.
	 */
	public static final int UPDATE_ONLY = 2;

	/**
	 * Do not raise an error if a parsed modify operation cannot be applied.
	 * <p>
	 * This flag does not suppress wrong bin type errors, invalid UTF-8 errors, bad
	 * flag combinations, malformed CDT paths, or {@link #CREATE_ONLY} with a CDT
	 * {@code CTX}. A suppressed modify operation leaves the string unchanged; a
	 * modify expression evaluates to the source string.
	 */
	public static final int NO_FAIL = 4;
}
