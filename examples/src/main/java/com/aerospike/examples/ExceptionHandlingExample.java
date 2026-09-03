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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * Demonstrates the exception hierarchy: provoking the common failures on purpose, catching them
 * at a specific type, and catching a whole category at a parent type.
 *
 * <p>Every client exception extends {@link AerospikeException}, which extends
 * {@code RuntimeException}, so nothing here is a checked exception. The value of the hierarchy is
 * that you can catch at exactly the granularity you need: {@code BinTypeException} for one
 * condition, or {@code BinException} for every bin-level problem. See
 * {@code docs/exception-hierarchy.md} for the full tree and the result-code table.</p>
 */
public class ExceptionHandlingExample extends Example {
    public static final String SET = "exception-demo";

    @Override
    public void runExample() throws Exception {
        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet users = dataSet(SET);

        Key alice = users.id("alice");
        Key missing = users.id("does-not-exist");

        session.insert(alice).bin("name").setTo("Alice").bin("visits").setTo(3).execute();

        console.info("--- 1) insert on an existing record -> RecordExistsException ---");
        try {
            session.insert(alice).bin("name").setTo("Alice again").execute();
            throw new AssertionError("Expected RecordExistsException");
        }
        catch (AerospikeException.RecordExistsException e) {
            describe(e);
        }

        console.info("--- 2) update on a missing record -> RecordNotFoundException ---");
        try {
            session.update(missing).bin("name").setTo("Nobody").execute();
            throw new AssertionError("Expected RecordNotFoundException");
        }
        catch (AerospikeException.RecordNotFoundException e) {
            describe(e);
        }

        console.info("--- 3) arithmetic on a string bin -> BinTypeException, caught as BinException ---");
        try {
            session.update(alice).bin("name").add(1).execute();
            throw new AssertionError("Expected a bin-level exception");
        }
        catch (AerospikeException.BinException e) {
            // Catching the parent handles BinExists, BinNotFound, BinType and BinOpInvalid alike.
            console.info("  caught as BinException; actual type is " + e.getClass().getSimpleName());
            describe(e);
        }

        console.info("--- 4) generation mismatch -> GenerationException ---");
        Record current = session.query(alice).execute().getFirst().orElseThrow().recordOrThrow();
        int staleGeneration = current.generation + 99;

        try {
            session.update(alice)
                    .ensureGenerationIs(staleGeneration)
                    .bin("visits").add(1)
                    .execute();
            throw new AssertionError("Expected GenerationException");
        }
        catch (AerospikeException.GenerationException e) {
            describe(e);
        }

        console.info("--- 5) filter expression excludes the record: silent by default ---");
        // A filter that doesn't match is not an error by default. The write is skipped and
        // nothing is thrown, so a single-key call cannot tell "filtered" from "applied" here.
        session.update(alice)
                .where(Exp.gt(Exp.intBin("visits"), Exp.val(1_000_000)))
                .bin("visits").add(1)
                .execute();
        console.info("  no exception; the write was silently skipped");

        console.info("--- 6) ...and opt in with failOnFilteredOut() -> FilteredException ---");
        try {
            session.update(alice)
                    .failOnFilteredOut()
                    .where(Exp.gt(Exp.intBin("visits"), Exp.val(1_000_000)))
                    .bin("visits").add(1)
                    .execute();
            throw new AssertionError("Expected FilteredException");
        }
        catch (AerospikeException.FilteredException e) {
            // Rejected by the filter, not by a failure. Usually means "precondition not met".
            describe(e);
        }

        console.info("--- 7) Mapping a raw result code to its exception type ---");
        AerospikeException mapped =
                AerospikeException.toException(ResultCode.KEY_NOT_FOUND_ERROR, "from a batch result");
        console.info("  code " + ResultCode.KEY_NOT_FOUND_ERROR
                + " maps to " + mapped.getClass().getSimpleName());

        console.info("Overall: SUCCESS");
    }

    private void describe(AerospikeException e) {
        console.info("  " + e.getClass().getSimpleName()
                + " (code " + e.getResultCode() + ", inDoubt=" + e.getInDoubt() + "): "
                + e.getBaseMessage());
    }
}
