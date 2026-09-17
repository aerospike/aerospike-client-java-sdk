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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;

public class TxnCommandTest {
    @Test
    public void markCommitFailedFromOpen() {
        Txn txn = new Txn();
        txn.markCommitFailed();
        assertEquals(Txn.State.COMMIT_FAILED, txn.getState());
    }

    @Test
    public void markCommitFailedFromVerified() {
        Txn txn = new Txn();
        txn.setState(Txn.State.VERIFIED);
        txn.markCommitFailed();
        assertEquals(Txn.State.COMMIT_FAILED, txn.getState());
    }

    @Test
    public void markCommitFailedPreservesAborted() {
        Txn txn = new Txn();
        txn.setState(Txn.State.ABORTED);
        txn.markCommitFailed();
        assertEquals(Txn.State.ABORTED, txn.getState());
    }

    @Test
    public void markCommitFailedPreservesCommitted() {
        Txn txn = new Txn();
        txn.setState(Txn.State.COMMITTED);
        txn.markCommitFailed();
        assertEquals(Txn.State.COMMITTED, txn.getState());
    }

    @Test
    public void verifyCommandRejectsCommitFailed() {
        Txn txn = new Txn();
        txn.setState(Txn.State.COMMIT_FAILED);

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            txn.verifyCommand();
        });

        assertEquals(ResultCode.TXN_FAILED, ae.getResultCode());
    }
}
