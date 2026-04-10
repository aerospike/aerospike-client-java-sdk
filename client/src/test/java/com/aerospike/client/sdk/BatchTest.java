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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;
import com.aerospike.client.sdk.util.Util;
import com.aerospike.client.sdk.util.Version;

public class BatchTest extends ClusterTest {
	private static final String BinName = "bbin";
	private static final String BinName2 = "bbin2";
	private static final String BinName3 = "bbin3";
	private static final String ListBin = "lbin";
	private static final String ListBin2 = "lbin2";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 10;

	@BeforeEach
	public void writeRecords() {
		int ttl = args.hasTtl? 2592000 : 0;

		for (int i = 1; i <= Size; i++) {
			String key = KeyPrefix + i;

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}

			List<Integer> list2 = new ArrayList<Integer>();

			for (int j = 0; j < 2; j++) {
				list2.add(j);
			}

			if (i != 6) {
		        session.upsert(args.set.id(key))
		        	.expireRecordAfterSeconds(ttl)
			        .bins(BinName, ListBin, ListBin2)
			        .values(ValuePrefix + i, list, list2)
			        .execute();
			}
			else {
		        session.upsert(args.set.id(key))
		        	.expireRecordAfterSeconds(ttl)
			        .bins(BinName, ListBin, ListBin2)
			        .values(i, list, list2)
			        .execute();
			}
		}

		// Add records that will eventually be deleted.
		int[] keys = new int[10];
		int firstKey = 10000;

		for (int i = 0; i < keys.length; i++) {
			int key = firstKey + i;

	        session.upsert(args.set.id(key))
		    	.expireRecordAfterSeconds(ttl)
		        .bin(BinName).setTo(key)
		        .execute();
		}
	}

	@Test
	public void batchExists () {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

        RecordStream recs = session.exists(args.set.ids(keys)).includeMissingKeys().execute();
        List<Boolean> exists = recs.stream().map(rec -> rec.asBoolean()).toList();
		assertEquals(Size, exists.size());

		for (int i = 0; i < Size; i++) {
			assertTrue(exists.get(i), "exists[" + i + "] is false");
		}
	}

	@Test
	public void batchReads () {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

        RecordStream rs = session.query(args.set.ids(keys))
           	.readingOnlyBins(BinName)
        	.execute();

		for (int i = 0; i < Size; i++) {
	        assertTrue(rs.hasNext());
	        Record rec = rs.next().recordOrThrow();

			if (i != 5) {
				String val = rec.getString(BinName);
				assertEquals(ValuePrefix + (i + 1), val);
			}
			else {
		        int val = rec.getInt(BinName);
				assertEquals(i + 1, val);
			}
		}

        assertFalse(rs.hasNext());
	}

	@Test
	public void shouldOmitKeyUnlessIncludeMissingKeysInBatchRead() {
		String presentKey = KeyPrefix + 1;
		String missingKey = "missing-key-12yz";
		session.delete(args.set.id(missingKey)).execute();
		// Default batch read: missing keys are not published to the stream.
		RecordStream rs = session
				.query(args.set.id(presentKey))
				.readingOnlyBins(BinName)
				.query(args.set.id(missingKey))
				.readingOnlyBins(BinName)
				.execute();

		List<RecordResult> resExcludingMissingKeys = rs.stream().toList();
		assertEquals(1, resExcludingMissingKeys.size());
		assertEquals(ResultCode.OK, resExcludingMissingKeys.getFirst().resultCode());
		assertEquals(ValuePrefix + "1", resExcludingMissingKeys.getFirst().recordOrThrow().getString(BinName));

		// includeMissingKeys: each missing key produces a RecordResult with KEY_NOT_FOUND_ERROR.
		rs = session
				.query(args.set.id(presentKey))
				.readingOnlyBins(BinName)
				.query(args.set.id(missingKey))
				.readingOnlyBins(BinName)
				.includeMissingKeys()
				.execute();

		List<RecordResult> resWithMissingKey = rs.stream().toList();
		assertEquals(2, resWithMissingKey.size());
		assertEquals(ResultCode.OK, resWithMissingKey.get(0).resultCode());
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, resWithMissingKey.get(1).resultCode());
	}

	@Test
	public void batchReadsEmptyBinNames() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

		String[] binNames = new String[] {};

		assertThrows(IllegalArgumentException.class, () -> {
	        session.query(args.set.ids(keys))
	           	.readingOnlyBins(binNames)
	        	.execute();
		});
	}

	@Test
	public void batchReadHeaders() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

        RecordStream rs = session.query(args.set.ids(keys))
    		.withNoBins()
        	.execute();

		for (int i = 0; i < Size; i++) {
	        assertTrue(rs.hasNext());
	        Record rec = rs.next().recordOrThrow();

			assertNotNull(rec);
			assertNotEquals(0, rec.generation);

			if (args.hasTtl) {
				assertNotEquals(0, rec.expiration);
			}
		}
        assertFalse(rs.hasNext());
	}

    @Test
	public void batchReadComplex() {
		String ael = "$.bbin * 8";

		RecordStream rs = session
			.query(args.set.id(KeyPrefix + 1))
				.readingOnlyBins(BinName)
			.query(args.set.id(KeyPrefix + 2))
			.query(args.set.id(KeyPrefix + 3))
				.withNoBins()
			.query(args.set.id(KeyPrefix + 4))
			.query(args.set.id(KeyPrefix + 6))
				.bin(BinName).selectFrom(ael)
			.query(args.set.id(KeyPrefix + 7))
				.readingOnlyBins("binnotfound")
			.query(args.set.id("keynotfound"))
				.readingOnlyBins(BinName)
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        String val = rec.getString(BinName);
        assertEquals("batchvalue1", val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(BinName);
        assertEquals("batchvalue2", val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(BinName);
        assertNull(val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(BinName);
        assertEquals("batchvalue4", val);

        int ival;

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        ival = rec.getInt(BinName);
        assertEquals(48, ival);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(BinName);
        assertNull(val);

        // TODO: Should hasNext() return true here?
        assertFalse(rs.hasNext());
	}

/* TODO Need list size to be supported.
	@Test
	public void batchListReadOperate() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

		// Get size and last element of list bin for all records.
        RecordStream rs = session.query(args.set.ids(keys))
        	.execute();


        Record[] records = client.get(null, keys,
			ListOperation.size(ListBin),
			ListOperation.getByIndex(ListBin, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < records.length; i++) {
			Record record = records[i];
			List<?> results = record.getList(ListBin);
			long size = (Long)results.get(0);
			long val = (Long)results.get(1);

			assertEquals(i + 1, size);
			assertEquals(i * (i + 1), val);
		}
	}
*/

/* TODO How get list size()?
	@Test
	public void batchListWriteOperate() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

		// Add integer to list and get size and last element of list bin for all records.
        session.upsert(args.set.ids(keys))
	    	.bin(ListBin2).onListIndex(0).listAdd(1000)
	    	// TO
	    	.bin(ListBin2).listSize??(0)
	    	.execute();

		BatchResults bresults = client.operate(null, null, keys,
			ListOperation.insert(ListBin2, 0, Value.get(1000)),
			ListOperation.size(ListBin2),
			ListOperation.getByIndex(ListBin2, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < bresults.records.length; i++) {
			BatchRecord br = bresults.records[i];
			assertEquals(0, br.resultCode);

			List<?> results = br.record.getList(ListBin2);
			long size = (Long)results.get(1);
			long val = (Long)results.get(2);

			assertEquals(3, size);
			assertEquals(1, val);
		}
	}
*/

/* TODO How retrieve all bins in operation Operation.get()?
	@Test
	public void batchReadAllBins() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

        session.upsert(args.set.ids(keys))
	    	.bin("bin5").setTo("NewValue")

	    	// TODO
	    	.ops()
	    	.execute();

        Bin bin = new Bin("bin5", "NewValue");

		BatchResults bresults = client.operate(null, null, keys,
			Operation.put(bin),
			Operation.get()
			);

		for (int i = 0; i < bresults.records.length; i++) {
			BatchRecord br = bresults.records[i];
			assertEquals(0, br.resultCode);

			Record r = br.record;

			String s = r.getString(bin.name);
			assertEquals(s, "NewValue");

			Object obj = r.getValue(BinName);
			assertNotNull(obj);
		}
	}
*/
	@Test
	public void batchWriteComplex() {
		DataSet ds = new DataSet("invalid", args.set.getSet());

		RecordStream rs = session
			.upsert(args.set.id(KeyPrefix + 1))
				.bin(BinName2).setTo(100)
			.upsert(ds.id(KeyPrefix + 1))
				.bin(BinName2).setTo(100)
			.upsert(args.set.id(KeyPrefix + 6))
				.bin(BinName3).upsertFrom("$.bbin + 1000")
			.delete(args.set.id(10002))
			.notInAnyTransaction()
			.execute();

        assertTrue(rs.hasNext());
        RecordResult res = rs.next();
		assertEquals(ResultCode.OK, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.INVALID_NAMESPACE, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.OK, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.OK, res.resultCode());

        assertFalse(rs.hasNext());

		rs = session
			.query(args.set.id(KeyPrefix + 1))
				.readingOnlyBins(BinName2)
			.query(args.set.id(KeyPrefix + 6))
				.readingOnlyBins(BinName3)
			.query(args.set.id(10002))
			.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(BinName2);
        assertEquals(100, val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getInt(BinName3);
        assertEquals(1006, val);

        // TODO: Should hasNext() return true here?
        assertFalse(rs.hasNext());
	}

	@Test
	public void batchDelete() {
		// Define keys
		int[] keys = new int[10];
		int firstKey = 10000;

		for (int i = 0; i < keys.length; i++) {
			keys[i] = firstKey + i;
		}

		// Ensure keys exists
        RecordStream recs = session.exists(args.set.ids(keys)).includeMissingKeys().execute();
        List<Boolean> exists = recs.stream().map(rec ->rec.asBoolean()).toList();
        assertEquals(keys.length, exists.size());

		for (boolean status : exists) {
			assertTrue(status);
		}

		// Delete keys
        List<Boolean> deletes = session.delete(args.set.ids(keys))
                .includeMissingKeys()
                .execute()
                .stream()
                .map(rec ->rec.asBoolean())
                .toList();
        assertEquals(keys.length, deletes.size());

		for (boolean status : deletes) {
			assertTrue(status);
		}

		// Ensure keys do not exist
        exists = session.exists(args.set.ids(keys))
                .includeMissingKeys()
                .execute()
                .stream()
                .map(rec ->rec.asBoolean())
                .toList();
        assertEquals(keys.length, exists.size());

        for (boolean status : exists) {
        	assertFalse(status);
		}
	}

	@Test
	public void batchDeleteSingleNotFound() {
		int[] keys = new int[10];
		int firstKey = 98929923; // Should be not found.

		for (int i = 0; i < 10; i++) {
			keys[i] = firstKey + i;
		}

        RecordStream rs = session.delete(args.set.ids(keys)).execute();
    	assertTrue(rs.hasNext());
    	assertFalse(rs.next().asBoolean());
	}

	@Test
	public void batchReadTTL() {
		Assumptions.assumeTrue(args.hasTtl);
		Assumptions.assumeTrue(args.serverVersion.compareTo(new Version(7, 0, 0, 0)) >= 0, "Skipping for server version less than 7.0");

		// WARNING: This test takes a long time to run due to sleeps.
		// Define keys
		Key key1 = args.set.id(88888);
		Key key2 = args.set.id(88889);

		// Write keys with ttl.
		List<Key> keys = List.of(key1, key2);

        session.upsert(keys)
	    	.expireRecordAfterSeconds(5)
	    	.bin("a").setTo(1)
	    	.execute();

		Util.sleep(3000);

		// Read records before they expire and reset read ttl on one record.
		Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
            .on(Selectors.reads(), ops -> ops
            	.resetTtlOnReadAtPercent(80)
            )
        );

        // Use local session to change default behavior.
        Session session1 = cluster.createSession(behavior1);

	    RecordStream rs = session1
        	.query(key1)
        	.readingOnlyBins("a")
            .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
    	int val = rec.getInt("a");
		assertEquals(1, val);

        Behavior behavior2 = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
            .on(Selectors.reads(), ops -> ops
            	.resetTtlOnReadAtPercent(-1)
            )
        );

	    Session session2 = cluster.createSession(behavior2);

	    rs = session2
        	.query(key2)
        	.readingOnlyBins("a")
            .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
    	val = rec.getInt("a");
		assertEquals(1, val);

		Util.sleep(3000);

		// Read records again, but don't reset read ttl.
	    rs = session2
        	.query(keys)
        	.readingOnlyBins("a")
            .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
    	val = rec.getInt("a");
		assertEquals(1, val);

		// Key 2 should have expired.
        assertFalse(rs.hasNext());

		Util.sleep(3000);

		// Read records again and both should be expired.
	    rs = session2
        	.query(keys)
        	.readingOnlyBins("a")
            .execute();

		// All keys should have expired.
        assertFalse(rs.hasNext());
	}

	@Test
	public void batchReadMixedExpressionsInvalidRowReturnsParameterError() {
		String key1 = KeyPrefix + 1;
		String key2 = KeyPrefix + 2;

		// Build a valid expression for one row and garbage bytes for another.
		Expression validExp = Exp.build(Exp.binExists(BinName));
		Expression invalidExp = Expression.fromBytes(new byte[] {0x00, 0x01, 0x02});

		RecordStream rs = session
			.query(args.set.id(key1))
				.where(validExp)
			.query(args.set.id(key2))
				.where(invalidExp)
			.includeMissingKeys()
			.execute();

		assertTrue(rs.hasNext());
		RecordResult res1 = rs.next();
		assertEquals(ResultCode.OK, res1.resultCode());

		assertTrue(rs.hasNext());
		RecordResult res2 = rs.next();
		assertEquals(ResultCode.PARAMETER_ERROR, res2.resultCode());
	}

	@Test
	public void batchReadWithInvalidExpressionReturnsParameterError() {
		String key1 = KeyPrefix + 1;
		String key2 = KeyPrefix + 2;

		Expression invalidExp = Expression.fromBytes(new byte[] {(byte)0xFF, (byte)0xFE, (byte)0xFD});
		List<Key> keys = args.set.ids(List.of(key1, key2));

		try {
			RecordStream rs = session.query(keys)
				.where(invalidExp)
				.includeMissingKeys()
				.execute();

			boolean foundParamError = false;
			while (rs.hasNext()) {
				RecordResult res = rs.next();
				if (res.resultCode() == ResultCode.PARAMETER_ERROR) {
					foundParamError = true;
				}
			}
			assertTrue(foundParamError,
				"Expected at least one PARAMETER_ERROR result from batch with invalid expression");
		} catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode(),
				"Expected PARAMETER_ERROR, got: " + ResultCode.getResultString(ae.getResultCode()));
		}
	}
}
