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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.CommitError;
import com.aerospike.client.sdk.command.TxnStatus;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * How a commit that could not finish is reported.
 * <p>
 * Committing sends three commands: mark-roll-forward, the roll-forward batch, then the monitor
 * close. Only the last two leave the transaction's writes durable, so which one fails decides
 * whether the caller is looking at a completed transaction. These tests route the client through
 * a {@link TcpGate} and let a chosen number of commands through, so a specific one fails.
 * <p>
 * A mark-roll-forward that fails in doubt leaves the transaction COMMIT_FAILED, which
 * {@link TransactionalSession} retries. The retry tests drop that one command and let the next
 * attempt through.
 */
public class TxnCommitStatusTest extends ClusterTest {
    private static final String binName = "bin";

    /** Let mark-roll-forward through, so the roll-forward batch is the command that fails. */
    private static final int THROUGH_MARK_ROLL_FORWARD = 1;

    /** Also let the roll-forward through, so the monitor close is the command that fails. */
    private static final int THROUGH_ROLL_FORWARD = 2;

    /** Let nothing through, so mark-roll-forward is the command that is dropped. */
    private static final int DROP_MARK_ROLL_FORWARD = 0;

    /**
     * One call attempt for the commit commands. Otherwise mark-roll-forward retries itself on a
     * new connection and succeeds, and the failure never reaches the commit retry under test.
     */
    private static final Behavior SINGLE_ATTEMPT_TXN_ROLL = Behavior.DEFAULT.deriveWithChanges(
        "singleAttemptTxnRoll", builder -> builder
            .on(Selectors.transaction().txnRoll(), ops -> ops.maximumNumberOfCallAttempts(1)));

    @BeforeAll
    public static void requireSC() {
        assumeTrue(args.scMode, "Transactions require strong consistency namespaces");
    }

    @Test
    public void abandonedRollForwardIsReportedToTheCaller() throws IOException {
        withGatedSession((gate, gated, key) -> {
            TxnStatus status = gated.doInTransaction(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.refuseAfterClientMessages(THROUGH_MARK_ROLL_FORWARD);
            });

            // The writes are still provisional here. Reporting OK would present them as committed.
            assertEquals(TxnStatus.ROLL_FORWARD_ABANDONED, status);
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
            TxnStatus status = gated.doInTransaction(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.refuseAfterClientMessages(THROUGH_ROLL_FORWARD);
            });

            // The roll-forward landed, so the writes are durable. Only the server side cleanup of
            // the transaction monitor was left undone, which is not the caller's problem.
            assertEquals(TxnStatus.ROLL_FORWARD_CLOSE_ABANDONED, status);
        });
    }

    @Test
    public void undisturbedCommitReportsOk() throws IOException {
        withGatedSession((gate, gated, key) -> {
            TxnStatus status = gated.doInTransaction(txn ->
                txn.update(key).bin(binName).add(1).execute());

            assertEquals(TxnStatus.COMMITTED, status);
        });
    }

    @Test
    public void inDoubtMarkRollForwardIsRetriedAndCommits() throws IOException {
        withGatedSession(SINGLE_ATTEMPT_TXN_ROLL, def -> {}, (gate, gated, key) -> {
            TxnStatus status = gated.doInTransaction(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.dropAfterClientMessages(DROP_MARK_ROLL_FORWARD);
            });

            // The first commit attempt really failed, so COMMITTED can only come from a retry.
            assertEquals(1, gate.droppedMessageCount());
            assertEquals(TxnStatus.COMMITTED, status);
            assertEquals(1, readBin(gated, key));
        });
    }

    @Test
    public void inDoubtMarkRollForwardIsRetriedWhenTheResultIsTheOperationsOwn() throws IOException {
        withGatedSession(SINGLE_ATTEMPT_TXN_ROLL, def -> {}, (gate, gated, key) -> {
            String result = gated.doInTransactionReturning(txn -> {
                txn.update(key).bin(binName).add(1).execute();
                gate.dropAfterClientMessages(DROP_MARK_ROLL_FORWARD);
                return "result";
            });

            assertEquals(1, gate.droppedMessageCount());
            assertEquals("result", result);
            assertEquals(1, readBin(gated, key));
        });
    }

    @Test
    public void inDoubtMarkRollForwardIsThrownWhenNoAttemptsRemain() throws IOException {
        // The same failure as above, with no attempts left to retry it. This shows the retry is
        // what turned it into a commit in the tests above.
        Consumer<ClusterDefinition> oneAttempt = def -> def.withSystemSettings(builder -> builder
            .transactions(ops -> ops.numberOfAttempts(1)));

        withGatedSession(SINGLE_ATTEMPT_TXN_ROLL, oneAttempt, (gate, gated, key) -> {
            AerospikeException.Commit thrown = assertThrows(AerospikeException.Commit.class,
                () -> gated.doInTransaction(txn -> {
                    txn.update(key).bin(binName).add(1).execute();
                    gate.dropAfterClientMessages(DROP_MARK_ROLL_FORWARD);
                }));

            assertEquals(1, gate.droppedMessageCount());
            assertEquals(CommitError.MARK_ROLL_FORWARD_ABANDONED, thrown.error);
            assertTrue(thrown.getInDoubt(), "dropped mark-roll-forward should be in doubt");
        });
    }

    private static long readBin(Session session, Key key) {
        RecordStream rs = session.query(key).execute();
        return rs.next().recordOrThrow().getLong(binName);
    }

    /**
     * Runs {@code transaction} against a session whose connections pass through a gate, whose key
     * already exists so the transaction can update it.
     */
    private void withGatedSession(GatedTransaction transaction) throws IOException {
        withGatedSession(Behavior.DEFAULT, def -> {}, transaction);
    }

    private void withGatedSession(
        Behavior behavior, Consumer<ClusterDefinition> configure, GatedTransaction transaction
    ) throws IOException {
        try (TcpGate gate = TcpGate.open(args.host, args.port)) {
            // Without this the client would take the node's advertised address from the partition
            // map and connect straight to it, leaving the gate with nothing to intercept.
            ClusterDefinition def = new ClusterDefinition("127.0.0.1", gate.getPort())
                .forceSingleNode(true);
            configure.accept(def);
            Cluster gatedCluster = def.connect();

            try {
                Session gated = gatedCluster.createSession(behavior);

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
