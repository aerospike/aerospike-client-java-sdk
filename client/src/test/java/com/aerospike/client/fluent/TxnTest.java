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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnRoll;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Settings;

public class TxnTest extends ClusterTest {
	private static final String binName = "bin";

	@BeforeAll
	public static void requireSC() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");
	}

	@Test
	public void txnWrite() {
		Key key = args.set.id("txnWrite");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
        	RecordStream rs = txnSession.upsert(key)
				.bins(binName)
				.values("val2")
				.execute();

     		assertTrue(rs.hasNext());
    		rs.next().recordOrThrow();
        });

		RecordStream rs = session.query(key).execute();
		Record rec = rs.next().recordOrThrow();
		assertEquals("val2", rec.getString(binName));
	}

	@Test
	public void txnWriteTwice() {
		Key key = args.set.id("txnWriteTwice");

        session.doInTransaction(txnSession -> {
			txnSession.upsert(key)
				.bins(binName)
				.values("val1")
				.execute();

			txnSession.upsert(key)
				.bins(binName)
				.values("val2")
				.execute();
	    });

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));
	}

	@Test
	public void txnWriteConflict() {
		Key key = args.set.id("txnWriteConflict");

        session.doInTransaction(txnSession1 -> {
    		txnSession1.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

    		session.doInTransaction(txnSession2 -> {
    			AerospikeException ae = assertThrows(AerospikeException.class, () -> {
    		        RecordStream rs = txnSession2.upsert(key)
						.bins(binName)
						.values("val2")
						.execute();

    		        assertTrue(rs.hasNext());
    		        rs.next().recordOrThrow();
    			});
    			assertEquals(ResultCode.MRT_BLOCKED, ae.getResultCode());
    		});
	    });

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnReadFailsForAllStatesExceptOpen() {
		Object[][] testCases = new Object[][] {
			{ Txn.State.OPEN, null },
			{ Txn.State.COMMITTED, "it has been committed" },
			{ Txn.State.ABORTED, "it has been aborted" },
			{ Txn.State.VERIFIED, "it is currently being committed" }
		};
		Key key = args.set.id("txnReadFailsForAllStatesExceptOpen");

		for (Object[] testCase : testCases) {
			Txn.State state = (Txn.State) testCase[0];
			String expectedMessage = (String) testCase[1];

			TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
			Txn txn = txnSession.getCurrentTransaction();
			txn.setState(state);

			if (expectedMessage == null) {
				try {
					RecordStream rs = txnSession.query(key).execute();
					if (rs.hasNext()) {
						rs.next();
					}
				} catch (AerospikeException ex) {
					fail("Did not expect exception for state " + state + " but got " + ex);
				}
			} else {
				try {
					RecordStream rs = txnSession.query(key).execute();
					if (rs.hasNext()) {
						rs.next();
					}
					fail("Expected AerospikeException for state " + state);
				} catch (AerospikeException ex) {
					if (!ex.getMessage().contains(expectedMessage)) {
						fail("Expected message containing '" + expectedMessage + "' for state " + state +
							" but got: " + ex.getMessage());
					}
				}
			}
		}
	}

	@Test
	public void txnWriteBlock() {
		Key key = args.set.id("txnWriteBlock");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
        	txnSession.upsert(key)
			.bins(binName)
			.values("val2")
			.execute();

   			AerospikeException ae = assertThrows(AerospikeException.class, () -> {
		        RecordStream rs = session.upsert(key)
					.bins(binName)
					.values("val3")
					.execute();

		        assertTrue(rs.hasNext());
		        rs.next().recordOrThrow();
			});
			assertEquals(ResultCode.MRT_BLOCKED, ae.getResultCode());
	    });
	}

	@Test
	public void txnWriteRead() {
		Key key = args.set.id("txnWriteRead");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
			txnSession.upsert(key)
				.bins(binName)
				.values("val2")
				.execute();

			RecordStream rs = session.query(key).execute();
			Record rec = rs.next().recordOrThrow();
			assertEquals("val1", rec.getString(binName));
	    });

        RecordStream rs = session.query(key).execute();
        Record rec = rs.next().recordOrThrow();
		assertEquals("val2", rec.getString(binName));
	}

	@Test
	public void txnWriteAbort() {
		// TODO It's not possible to call doInTransaction() in this test because that method
		// implicitly calls commit when finished. This test explicitly calls abort() and must
		// use low level transactions calls to perform the abort (awkward).
		Key key = args.set.id("mrtkey5");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.upsert(key)
			.bins(binName)
			.values("val2")
			.execute();

		RecordStream rs = txnSession.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));

		abortTxn(txn);

		rs = session.query(key).execute();
		record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnDelete() {
		Key key = args.set.id("txnDelete");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
    		txnSession.delete(key)
				.withDurableDelete()
				.execute();
	    });

		RecordStream rs = session.query(key).execute();
        assertFalse(rs.hasNext());
	}

	@Test
	public void txnDeleteAbort() {
		// TODO It's not possible to call doInTransaction() in this test because that method
		// implicitly calls commit when finished. This test explicitly calls abort() and must
		// use low level transactions calls to perform the abort (awkward).
		Key key = args.set.id("txnDeleteAbort");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.delete(key)
			.withDurableDelete()
			.execute();

		abortTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record rec = rs.next().recordOrThrow();
		assertEquals("val1", rec.getString(binName));
	}

	@Test
	public void txnDeleteTwice() {
		Key key = args.set.id("txnDeleteTwice");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
        	RecordStream rs = txnSession.delete(key)
				.withDurableDelete()
				.execute();

    		assertTrue(rs.hasNext());
    		rs.next().recordOrThrow();

    		rs = txnSession.delete(key)
				.withDurableDelete()
				.execute();

    		assertTrue(rs.hasNext());
    		rs.next().recordOrNull();
        });

		RecordStream rs = session.query(key).execute();
		assertFalse(rs.hasNext());
	}

	@Test
	public void txnTouch() {
		Key key = args.set.id("txnTouch");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

        session.doInTransaction(txnSession -> {
        	txnSession.touch(key).execute();
        });

		RecordStream rs = session.query(key).execute();
		Record rec = rs.next().recordOrThrow();
		assertEquals("val1", rec.getString(binName));
	}

	@Test
	public void txnTouchAbort() {
		// TODO It's not possible to call doInTransaction() in this test because that method
		// implicitly calls commit when finished. This test explicitly calls abort() and must
		// use low level transactions calls to perform the abort (awkward).
		Key key = args.set.id("txnTouchAbort");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.touch(key).execute();

		abortTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record rec = rs.next().recordOrThrow();
		assertEquals("val1", rec.getString(binName));
	}

	@Test
	public void txnOperateWrite() {
		Key key = args.set.id("txnOperateWrite");

		session.upsert(key)
			.bins(binName, "bin2")
			.values("val1", "bal1")
			.execute();

        session.doInTransaction(txnSession -> {
    		RecordStream rs = txnSession.upsert(key)
				.bin(binName).setTo("val2")
				.get("bin2")
				.execute();

			Record rec = rs.next().recordOrThrow();
			assertEquals("bal1", rec.getString("bin2"));
       });

        RecordStream rs = session.query(key).execute();
        Record rec = rs.next().recordOrThrow();
		assertEquals("val2", rec.getString(binName));
	}

	@Test
	public void txnOperateWriteAbort() {
		// TODO It's not possible to call doInTransaction() in this test because that method
		// implicitly calls commit when finished. This test explicitly calls abort() and must
		// use low level transactions calls to perform the abort (awkward).
		Key key = args.set.id("txnOperateWriteAbort");

		session.upsert(key)
			.bins(binName, "bin2")
			.values("val1", "bal1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		RecordStream rs = txnSession.upsert(key)
			.bin(binName).setTo("val2")
			.get("bin2")
			.execute();

		Record rec = rs.next().recordOrThrow();
		assertEquals("bal1", rec.getString("bin2"));

		abortTxn(txn);

		rs = session.query(key).execute();
		rec = rs.next().recordOrThrow();
		assertEquals("val1", rec.getString(binName));
	}

	@Test
	public void txnBatch() {
		java.util.List<Key> keys = args.set.ids(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

		for (Key key : keys) {
			session.upsert(key)
				.bins(binName)
				.values(1)
				.execute();
		}

		RecordStream recs = session.query(keys).execute();
		assertBatchEqual(keys, recs, 1);

        session.doInTransaction(txnSession -> {
    		RecordStream bresults = txnSession.upsert(keys)
				.bin(binName).setTo(2)
				.execute();

			while (bresults.hasNext()) {
				RecordResult rr = bresults.next();
				if (rr.resultCode() != 0) {
					fail("Batch operation failed: " + rr.resultCode());
				}
			}
        });

		recs = session.query(keys).execute();
		assertBatchEqual(keys, recs, 2);
	}

	@Test
	public void txnBatchAbort() {
		// TODO It's not possible to call doInTransaction() in this test because that method
		// implicitly calls commit when finished. This test explicitly calls abort() and must
		// use low level transactions calls to perform the abort (awkward).
		java.util.List<Key> keys = args.set.ids(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);

		for (Key key : keys) {
			session.upsert(key)
				.bins(binName)
				.values(1)
				.execute();
		}

		RecordStream recs = session.query(keys).execute();
		assertBatchEqual(keys, recs, 1);

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		RecordStream bresults = txnSession.upsert(keys)
			.bin(binName).setTo(2)
			.execute();

		while (bresults.hasNext()) {
			RecordResult rr = bresults.next();
			if (rr.resultCode() != 0) {
				fail("Batch operation failed: " + rr.resultCode());
			}
		}

		abortTxn(txn);

		recs = session.query(keys).execute();
		assertBatchEqual(keys, recs, 1);
	}

	private void assertBatchEqual(java.util.List<Key> keys, RecordStream recs, int expected) {
		int count = 0;
		while (recs.hasNext()) {
			Record rec = recs.next().recordOrThrow();
			assertNotNull(rec);
			int received = rec.getInt(binName);
			assertEquals(expected, received);
			count++;
		}
		assertEquals(keys.size(), count);
	}

	private void abortTxn(Txn txn) {
		TxnRoll tr = new TxnRoll(cluster, txn);
		Settings rollPolicy = Behavior.DEFAULT.getSettings(
			Behavior.OpKind.SYSTEM_TXN_ROLL,
			Behavior.OpShape.SYSTEM,
			Behavior.Mode.ANY
		);

		switch (txn.getState()) {
			case OPEN:
			case VERIFIED:
				tr.abort(rollPolicy);
				break;
			case COMMITTED:
				throw AerospikeException.resultCodeToException(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed");
			case ABORTED:
				break;
		}
	}
}
