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

import java.time.LocalDateTime;
import java.util.Date;

import com.aerospike.client.fluent.command.Txn;

/**
 * Interface for operations that support the bins+values pattern.
 * This defines the methods that BinsValuesBuilder needs from its parent operation builder.
 */
interface BinsValuesOperations {
    /**
     * Get the session associated with this operation.
     */
    Session getSession();

    /**
     * Get the operation type.
     */
    OpType getOpType();

    /**
     * Get the transaction to use
     */
    Txn getTxnToUse();

    boolean getNotInAnyTransaction();

    /**
     * Get the number of keys in this operation.
     */
    int getNumKeys();

    /**
     * Check if this operation has multiple keys.
     */
    boolean isMultiKey();

    boolean isIncludeMissingKeys();

    boolean isFailOnFilteredOut();

    /**
     * Convert expiration in seconds to int, capping at Integer.MAX_VALUE.
     */
    int getExpirationAsInt(long expirationInSeconds);

    /**
     * Convert a Date to expiration in seconds and validate it.
     */
    long getExpirationInSecondsAndCheckValue(Date date);

    /**
     * Convert a LocalDateTime to expiration in seconds and validate it.
     */
    long getExpirationInSecondsAndCheckValue(LocalDateTime dateTime);

    /**
     * Show warnings for specific exception conditions.
     */
    void showWarningsOnException(AerospikeException ae, Txn txn, Key key, int expiration);
}

