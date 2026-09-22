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
package com.aerospike.client.sdk.command;

/**
 * Transaction commit status code.
 */
public enum TxnStatus {
    COMMITTED("Commit succeeded"),
    ABORTED("Abort succeeded"),
    ALREADY_COMMITTED("Already committed"),
    ALREADY_ABORTED("Already aborted"),
    VERIFY_FAILED("Transaction verify failed"),
    COMMIT_FAILED("Abort not allowed because a commit already failed on this transaction with an in-doubt outcome"),
    MARK_ROLL_FORWARD_ABANDONED("Transaction mark roll forward abandonded"),
    ROLL_FORWARD_ABANDONED("Transaction client roll forward abandoned. Server will eventually commit the transaction."),
    ROLL_FORWARD_CLOSE_ABANDONED("Transaction has been rolled forward, but transaction client close was abandoned. Server will eventually close the transaction."),
    ROLL_BACK_ABANDONED("Transaction client roll back abandoned. Server will eventually abort the transaction."),
    ROLL_BACK_CLOSE_ABANDONED("Transaction has been rolled back, but transaction client close was abandoned. Server will eventually close the transaction.");

    public final String str;

    TxnStatus(String str) {
        this.str = str;
    }
}
