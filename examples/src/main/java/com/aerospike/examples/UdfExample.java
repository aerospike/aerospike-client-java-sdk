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

import java.util.List;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordResult;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;

/**
 * Demonstrates user defined functions (UDFs): registering Lua on the cluster, executing a
 * record UDF against a single key, executing the same UDF across a batch of keys, chaining a
 * UDF with ordinary write operations, and surfacing a UDF failure as
 * {@link AerospikeException.UdfException}.
 *
 * <p>A record UDF runs on the node that owns the record, so read-modify-write logic that would
 * otherwise need two round-trips (and a race between them) executes atomically in one.</p>
 */
public class UdfExample extends Example {
    /** Server-side module name. Referenced by {@code function(PACKAGE, ...)}. */
    public static final String PACKAGE = "example_bonus";

    /** Path the module is registered under, relative to the server's UDF directory. */
    public static final String SERVER_PATH = PACKAGE + ".lua";

    public static final String SET = "udf-demo";

    /**
     * Registered from a string so the example is self-contained; production code more often ships
     * a {@code .lua} file and calls {@code registerUdf(clientPath, serverPath)}.
     */
    private static final String LUA_SOURCE = """
        local function balance_of(rec)
            return rec['balance'] or 0
        end

        -- Read-modify-write in one server-side step, returning the new balance.
        function apply_bonus(rec, amount)
            if not aerospike:exists(rec) then
                return nil
            end
            rec['balance'] = balance_of(rec) + amount
            rec['bonuses'] = (rec['bonuses'] or 0) + 1
            aerospike:update(rec)
            return rec['balance']
        end

        -- Read-only: derives a value without writing the record.
        function tier(rec)
            local balance = balance_of(rec)
            if balance >= 1000 then
                return 'GOLD'
            elseif balance >= 500 then
                return 'SILVER'
            end
            return 'BRONZE'
        end

        -- Always fails, to show how a Lua error reaches the client.
        function always_fails(rec)
            error('deliberate failure from Lua')
        end
        """;

    @Override
    public void runExample() throws Exception {
        Session session = cluster().createSession(Behavior.DEFAULT);
        DataSet accounts = dataSet(SET);

        console.info("--- 1) Register the Lua module ---");
        session.registerUdfString(LUA_SOURCE, SERVER_PATH).waitTillComplete();
        console.info("registered " + SERVER_PATH);

        session.upsert(accounts.id("acct-1")).bin("balance").setTo(100).execute();
        session.upsert(accounts.id("acct-2")).bin("balance").setTo(600).execute();
        session.upsert(accounts.id("acct-3")).bin("balance").setTo(1500).execute();

        console.info("--- 2) Single-key UDF: apply_bonus(50) on acct-1 ---");
        Object newBalance = session.executeUdf(accounts.id("acct-1"))
                .function(PACKAGE, "apply_bonus")
                .passing(50)
                .execute()
                .getFirstUdfResultObject()
                .orElseThrow();
        console.info("apply_bonus returned new balance: " + newBalance);

        console.info("--- 3) Batch UDF: apply_bonus(10) on three keys in one call ---");
        List<Key> all = accounts.ids("acct-1", "acct-2", "acct-3");

        try (RecordStream stream = session.executeUdf(all)
                .function(PACKAGE, "apply_bonus")
                .passing(10)
                .execute()) {
            while (stream.hasNext()) {
                RecordResult result = stream.next().orThrow();
                // In a batch the return value arrives in the record's "SUCCESS" bin, read via
                // getUDFResult(). The single-key udfResultOrThrow() accessor is not populated here.
                Object returned = result.recordOrThrow().getUDFResult();
                console.info("  " + result.getKey().userKey + " -> " + returned);
            }
        }

        console.info("--- 4) Read-only UDF: tier() derives a value without writing ---");
        for (Key key : all) {
            Object tier = session.executeUdf(key)
                    .function(PACKAGE, "tier")
                    .execute()
                    .getFirstUdfResultObject()
                    .orElseThrow();
            console.info("  " + key.userKey + " -> " + tier);
        }

        console.info("--- 5) Chain a UDF with an ordinary write in one round-trip ---");
        session.executeUdf(accounts.id("acct-2"))
                .function(PACKAGE, "apply_bonus")
                .passing(25)
                .upsert(accounts.id("acct-3"))
                .bin("note").setTo("audited")
                .execute();

        Record acct3 = session.query(accounts.id("acct-3"))
                .execute()
                .getFirst()
                .orElseThrow()
                .recordOrThrow();
        console.info("acct-3 after chained write: " + acct3.bins);

        console.info("--- 6) A Lua error surfaces as AerospikeException.UdfException ---");
        try {
            session.executeUdf(accounts.id("acct-1"))
                    .function(PACKAGE, "always_fails")
                    .execute();
            throw new AssertionError("Expected always_fails to raise UdfException");
        }
        catch (AerospikeException.UdfException e) {
            console.info("caught UdfException (code " + e.getResultCode() + "): " + e.getBaseMessage());
        }

        console.info("Overall: SUCCESS");
    }
}
