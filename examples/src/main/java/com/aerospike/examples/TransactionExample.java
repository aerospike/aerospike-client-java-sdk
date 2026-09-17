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
package com.aerospike.examples;

import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.tend.Partitions;

/**
 * Demonstrates multi-record transactions: {@code doInTransaction} for all-or-nothing writes,
 * {@code doInTransactionReturning} for reading a value out of a transaction, {@code abort()} to
 * roll back deliberately, and rollback triggered by an exception escaping the lambda.
 *
 * <p>The classic motivating case is a funds transfer: two records must both change or neither
 * may. Inside the lambda you get a {@code TransactionalSession} that carries the transaction
 * identity; every call made through it joins the transaction, and the commit happens when the
 * lambda returns normally.</p>
 *
 * <p>Multi-record transactions require a strong-consistency namespace. On an AP namespace this
 * example is skipped.</p>
 */
public class TransactionExample extends Example {
    public static final String SET = "txn-demo";

    @Override
    public void runExample() throws Exception {
        requireStrongConsistency();

        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet accounts = dataSet(SET);

        Key alice = accounts.id("alice");
        Key bob = accounts.id("bob");

        session.upsert(alice).bin("balance").setTo(1000).execute();
        session.upsert(bob).bin("balance").setTo(1000).execute();
        report(session, accounts, "opening balances");

        console.info("--- 1) Commit: transfer 250 from alice to bob ---");
        session.doInTransaction(txn -> {
            txn.update(alice).bin("balance").add(-250).execute();
            txn.update(bob).bin("balance").add(250).execute();
        });
        report(session, accounts, "after committed transfer");

        console.info("--- 2) doInTransactionReturning: read a value out of the transaction ---");
        long aliceBalance = session.doInTransactionReturning(txn -> {
            txn.update(alice).bin("balance").add(-100).execute();
            return balanceOf(txn, alice);
        });
        console.info("balance observed inside the transaction: " + aliceBalance);
        report(session, accounts, "after second transfer");

        console.info("--- 3) abort(): roll back deliberately when a precondition fails ---");
        session.doInTransaction(txn -> {
            txn.update(alice).bin("balance").add(-5000).execute();

            if (balanceOf(txn, alice) < 0) {
                // Nothing written in this lambda survives; abort() unwinds the whole transaction.
                txn.abort();
            }
        });
        report(session, accounts, "after aborted overdraft (unchanged)");

        console.info("--- 4) An exception escaping the lambda also rolls back ---");
        try {
            session.doInTransaction(txn -> {
                txn.update(alice).bin("balance").add(-1).execute();
                throw new IllegalStateException("simulated downstream failure");
            });
            throw new AssertionError("Expected the exception to propagate");
        }
        catch (IllegalStateException e) {
            console.info("caught " + e.getMessage() + "; transaction was rolled back");
        }
        report(session, accounts, "after failed transaction (unchanged)");

        console.info("Overall: SUCCESS");
    }

    private static long balanceOf(Session session, Key key) {
        return session.query(key)
                .execute()
                .getFirst()
                .orElseThrow()
                .recordOrThrow()
                .getLong("balance");
    }

    private void report(Session session, DataSet accounts, String label) {
        console.info(label + ": alice=" + balanceOf(session, accounts.id("alice"))
                + " bob=" + balanceOf(session, accounts.id("bob")));
    }

    /**
     * Multi-record transactions are only available on strong-consistency namespaces.
     */
    private void requireStrongConsistency() throws ExampleSkipException {
        Partitions partitions = cluster().getPartitionMap().get(namespace());

        if (partitions == null || !partitions.scMode) {
            throw new ExampleSkipException(
                "namespace '" + namespace() + "' is not strong-consistency; transactions require SC");
        }
    }
}
