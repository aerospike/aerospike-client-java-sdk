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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.CommitError;
import com.aerospike.client.sdk.command.CommitStatus;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * How a commit that could not finish is reported.
 * <p>
 * Committing sends three commands: mark-roll-forward, the roll-forward batch, then the monitor
 * close. Only the last two leave the transaction's writes durable, so which one fails decides
 * whether the caller is looking at a completed transaction. These tests route the client through
 * a {@link TcpGate} and let a chosen number of commands through, so a specific one fails.
 */
public class TxnCommitStatusTest extends ClusterTest {
    private static final String binName = "bin";

    /** Let mark-roll-forward through, so the roll-forward batch is the command that fails. */
    private static final int THROUGH_MARK_ROLL_FORWARD = 1;

    /** Also let the roll-forward through, so the monitor close is the command that fails. */
    private static final int THROUGH_ROLL_FORWARD = 2;

    @BeforeAll
    public static void requireSC() {
        assumeTrue(args.scMode, "Transactions require strong consistency namespaces");
    }

    @Test
    public void abandonedRollForwardIsReportedToTheCaller() throws IOException {
        withGatedSession((gate, gated, key) -> {
            CommitStatus status = gated.doInTransaction(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.refuseAfterClientMessages(THROUGH_MARK_ROLL_FORWARD);
            });

            // The writes are still provisional here. Reporting OK would present them as committed.
            assertEquals(CommitStatus.ROLL_FORWARD_ABANDONED, status);
        });
    }

    @Test
    public void abandonedRollForwardIsThrownWhenTheResultIsTheOperationsOwn() throws IOException {
        withGatedSession((gate, gated, key) -> {
            // This overload returns the operation's result, leaving no room for a status, so the
            // only way to report an unfinished commit is to throw.
            AerospikeException.Commit thrown = assertThrows(AerospikeException.Commit.class,
                () -> gated.doInTransactionReturning(txn -> {
                    txn.update(key).bin(binName).add(1).execute();
                    gate.refuseAfterClientMessages(THROUGH_MARK_ROLL_FORWARD);
                    return "ignored";
                }));

            assertEquals(CommitError.ROLL_FORWARD_ABANDONED, thrown.error);
        });
    }

    @Test
    public void abandonedMonitorCloseStillCommits() throws IOException {
        withGatedSession((gate, gated, key) -> {
            CommitStatus status = gated.doInTransaction(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.refuseAfterClientMessages(THROUGH_ROLL_FORWARD);
            });

            // The roll-forward landed, so the writes are durable. Only the server side cleanup of
            // the transaction monitor was left undone, which is not the caller's problem.
            assertEquals(CommitStatus.CLOSE_ABANDONED, status);
        });
    }

    @Test
    public void undisturbedCommitReportsOk() throws IOException {
        withGatedSession((gate, gated, key) -> {
            CommitStatus status = gated.doInTransaction(txn ->
                txn.update(key).bin(binName).add(1).execute());

            assertEquals(CommitStatus.OK, status);
        });
    }

    /**
     * Runs {@code transaction} against a session whose connections pass through a gate, whose key
     * already exists so the transaction can update it.
     */
    private void withGatedSession(GatedTransaction transaction) throws IOException {
        try (TcpGate gate = TcpGate.open(args.host, args.port)) {
            // Without this the client would take the node's advertised address from the partition
            // map and connect straight to it, leaving the gate with nothing to intercept.
            Cluster gatedCluster = new ClusterDefinition("127.0.0.1", gate.getPort())
                .forceSingleNode(true)
                .connect();

            try {
                Session gated = gatedCluster.createSession(Behavior.DEFAULT);

                // A fresh key each run: a transaction left unfinished by these tests keeps its
                // records locked until the server resolves it.
                Key key = args.set.id("commitStatus-" + System.nanoTime());
                gated.upsert(key).bin(binName).setTo(0).execute();

                transaction.run(gate, gated, key);
            }
            finally {
                gatedCluster.close();
            }
        }
    }

    @FunctionalInterface
    private interface GatedTransaction {
        void run(TcpGate gate, Session gated, Key key);
    }
}
