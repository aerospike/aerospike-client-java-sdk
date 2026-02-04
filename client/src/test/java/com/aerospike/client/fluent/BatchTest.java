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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BatchTest extends ClusterTest {
	private static final String BinName = "bbin";
	private static final String BinName2 = "bbin2";
	private static final String BinName3 = "bbin3";
	private static final String ListBin = "lbin";
	private static final String ListBin2 = "lbin2";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 10;

	@BeforeAll
	public static void writeRecords() {
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

        RecordStream recs = session.exists(args.set.ids(keys)).respondAllKeys().execute();
        List<Boolean> exists = recs.stream().map(rec ->rec.asBoolean()).toList();
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
	public void batchReadsEmptyBinNames() {
		List<String> keys = new ArrayList<>(Size);

		for (int i = 0; i < Size; i++) {
			keys.add(KeyPrefix + (i + 1));
		}

		String[] binNames = new String[] {};

        RecordStream rs = session.query(args.set.ids(keys))
           	.readingOnlyBins(binNames)
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
	public void batchReadHeaders () {
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

/* TODO How query multiple keys each with different ops.
	@Test
	public void batchReadComplex() {
		RecordStream rs = session
			.query(args.set.id(KeyPrefix + 1))
				.readingOnlyBins(BinName)
			.query(args.set.id(KeyPrefix + 2))
			.execute();

		// bin * 8
		Expression exp = Exp.build(Exp.mul(Exp.intBin(BinName), Exp.val(8)));
		Operation[] ops = Operation.array(ExpOperation.read(BinName, exp, ExpReadFlags.DEFAULT));

		String[] bins = new String[] {BinName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 3), new String[] {}));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 4), false));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 5), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 6), ops));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, "keynotfound"), bins));

		// Execute batch.
		client.get(null, records);

		assertBatchBinEqual(records, BinName, 0);
		assertBatchBinEqual(records, BinName, 1);
		assertBatchBinEqual(records, BinName, 2);
		assertBatchRecordExists(records, BinName, 3);
		assertBatchBinEqual(records, BinName, 4);

		BatchRead batch = records.get(5);
		assertRecordFound(batch.key, batch.record);
		int v = batch.record.getInt(BinName);
		assertEquals(48, v);

		assertBatchBinEqual(records, BinName, 6);

		batch = records.get(7);
		assertRecordFound(batch.key, batch.record);
		Object val = batch.record.getValue("binnotfound");
		if (val != null) {
			fail("Unexpected batch bin value received");
		}

		batch = records.get(8);
		if (batch.record != null) {
			fail("Unexpected batch record received");
		}
	}
*/
/* TODO Need batch read with operations api
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

/* TODO How retrieve all bins in operation?
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

	/* TODO Wait for external batch complex api*/
	 // BN TRY update instead of query.
	@Test
	public void batchWriteComplex() {
		DataSet ds = new DataSet("invalid", args.set.getSet());

		// Returns PARAMETER ERROR
		//Feb 03 2026 23:06:53 GMT: WARNING (batch): (batch.c:1110) batch request has unknown namespace
		//Feb 03 2026 23:06:53 GMT: WARNING (batch): (batch.c:1195) Batch keys mismatch. Expected 4 Received 1

		/*
        System.out.println("START batchWriteComplex");
		RecordStream rs = session
			.upsert(args.set.id(KeyPrefix + 1))
				.bin(BinName2).setTo(100)
			.upsert(ds.id(KeyPrefix + 1))
				.bin(BinName2).setTo(100)
			.upsert(args.set.id(KeyPrefix + 6))
				.bin(BinName3).upsertFrom("$.bbin + 1000")
			.delete(args.set.id(10002))
			.execute();
        System.out.println("END batchWriteComplex");

		Expression wexp1 = Exp.build(Exp.add(Exp.intBin(BinName), Exp.val(1000)));

		Operation[] wops1 = Operation.array(Operation.put(new Bin(BinName2, 100)));
		Operation[] wops2 = Operation.array(ExpOperation.write(BinName3, wexp1, ExpWriteFlags.DEFAULT));
		Operation[] rops1 = Operation.array(Operation.get(BinName2));
		Operation[] rops2 = Operation.array(Operation.get(BinName3));

		BatchWritePolicy wp = new BatchWritePolicy();
		wp.sendKey = true;

		BatchWrite bw1 = new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 1), wops1);
		BatchWrite bw2 = new BatchWrite(new Key("invalid", args.set, KeyPrefix + 1), wops1);
		BatchWrite bw3 = new BatchWrite(wp, new Key(args.namespace, args.set, KeyPrefix + 6), wops2);
		BatchDelete bd1 = new BatchDelete(new Key(args.namespace, args.set, 10002));

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(bw1);
		records.add(bw2);
		records.add(bw3);
		records.add(bd1);

		boolean status = client.operate(null, records);
		assertFalse(status);  // "invalid" namespace triggers the false status.

		assertEquals(ResultCode.OK, bw1.resultCode);
		assertBinEqual(bw1.key, bw1.record, BinName2, 0);

		assertEquals(ResultCode.INVALID_NAMESPACE, bw2.resultCode);

		assertEquals(ResultCode.OK, bw3.resultCode);
		assertBinEqual(bw3.key, bw3.record, BinName3, 0);

		assertEquals(ResultCode.OK, bd1.resultCode);

		BatchRead br1 = new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 1), rops1);
		BatchRead br2 = new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 6), rops2);
		BatchRead br3 = new BatchRead(new Key(args.namespace, args.set, 10002), true);

		records.clear();
		records.add(br1);
		records.add(br2);
		records.add(br3);

		status = client.operate(null, records);
		assertFalse(status); // Read of deleted record causes status to be false.

		assertBinEqual(br1.key, br1.record, BinName2, 100);
		assertBinEqual(br2.key, br2.record, BinName3, 1006);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br3.resultCode);
		*/
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
        RecordStream recs = session.exists(args.set.ids(keys)).respondAllKeys().execute();
        List<Boolean> exists = recs.stream().map(rec ->rec.asBoolean()).toList();
        assertEquals(keys.length, exists.size());

		for (boolean status : exists) {
			assertTrue(status);
		}

		// Delete keys
        List<Boolean> deletes = session.delete(args.set.ids(keys))
                .respondAllKeys()
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
                .respondAllKeys()
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
    	assertFalse(rs.hasNext());
	}

/* TODO How set different resetTtlOnReadAtPercent in same batch??
	@Test
	public void batchReadTTL() {
		Assumptions.assumeTrue(args.hasTtl);
		Assumptions.assumeTrue(args.serverVersion.compareTo(new Version(7, 0, 0, 0)) >= 0, "Skipping for server version less than 7.0");

		// WARNING: This test takes a long time to run due to sleeps.
		// Define keys
		int[] keys = new int[10];
		int firstKey = 88888;

		for (int i = 0; i < keys.length; i++) {
			keys[i] = firstKey + i;
		}

		int ttl = 10;

		// Write keys with ttl.
        session.upsert(args.set.ids(keys))
        	.expireRecordAfterSeconds(ttl)
	    	.bin("a").setTo(1)
	    	.execute();

		Util.sleep(8000);

		Behavior behavior1 = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
            .on(Selectors.reads(), ops -> ops
            	.resetTtlOnReadAtPercent(80)
            )
        );

        // Use local session to change default behavior.
        Session session1 = cluster.createSession(behavior1);

        Behavior behavior2 = Behavior.DEFAULT.deriveWithChanges("readttl", builder -> builder
                .on(Selectors.reads(), ops -> ops
                	.resetTtlOnReadAtPercent(-1)
                )
            );

        Session session2 = cluster.createSession(behavior2);

        // Read records before they expire and reset read ttl on one record.

        // TODO How set different resetTtlOnReadAtPercent in same batch??
        RecordStream rs = session1.query(args.set.ids(keys))
        	.readingOnlyBins("a")
            .execute();



		BatchRead br1 = new BatchRead(brp1, key1, new String[] {"a"});
		BatchRead br2 = new BatchRead(brp2, key2, new String[] {"a"});

		List<BatchRecord> list = new ArrayList<>();
		list.add(br1);
		list.add(br2);

		boolean rv = client.operate(null, list);

		assertEquals(ResultCode.OK, br1.resultCode);
		assertEquals(ResultCode.OK, br2.resultCode);
		assertTrue(rv);

		// Read records again, but don't reset read ttl.
		Util.sleep(3000);
		brp1.readTouchTtlPercent = -1;
		brp2.readTouchTtlPercent = -1;

		br1 = new BatchRead(brp1, key1, new String[] {"a"});
		br2 = new BatchRead(brp2, key2, new String[] {"a"});

		list.clear();
		list.add(br1);
		list.add(br2);

		rv = client.operate(null, list);

		// Key 2 should have expired.
		assertEquals(ResultCode.OK, br1.resultCode);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br2.resultCode);
		assertFalse(rv);

		// Read  record after it expires, showing it's gone.
		Util.sleep(8000);
		rv = client.operate(null, list);

		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br1.resultCode);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br2.resultCode);
		assertFalse(rv);
	}
*/
}
