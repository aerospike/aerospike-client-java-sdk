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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.command.Txn;
import com.aerospike.client.fluent.command.TxnRoll;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.policy.Settings;

public class TxnTest extends ClusterTest {
	private static final String binName = "bin";

	@Test
	public void txnWrite() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey1");

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

		commitTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));
	}

	@Test
	public void txnWriteTwice() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey2");

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		txnSession.upsert(key)
			.bins(binName)
			.values("val2")
			.execute();

		commitTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));
	}

	@Test
	public void txnWriteConflict() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey21");

		TransactionalSession txnSession1 = new TransactionalSession(cluster, Behavior.DEFAULT);
		TransactionalSession txnSession2 = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn1 = txnSession1.getCurrentTransaction();
		Txn txn2 = txnSession2.getCurrentTransaction();

		txnSession1.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		try {
			txnSession2.upsert(key)
				.bins(binName)
				.values("val2")
				.execute();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.MRT_BLOCKED) {
				throw ae;
			}
		}

		commitTxn(txn1);
		commitTxn(txn2);

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnReadFailsForAllStatesExceptOpen() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Object[][] testCases = new Object[][] {
			{ Txn.State.OPEN, null },
			{ Txn.State.COMMITTED, "it has been committed" },
			{ Txn.State.ABORTED, "it has been aborted" },
			{ Txn.State.VERIFIED, "it is currently being committed" }
		};
		Key key = args.set.id("mrtkey21");

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

	// txnWriteBlock - NOT MIGRATED
	// This test cannot be reliably migrated due to fundamental differences in transaction monitor
	// creation timing between the legacy and fluent APIs. The legacy API creates the transaction
	// monitor record synchronously with the first transactional write, ensuring the monitor exists
	// on the server before the write operation returns. The fluent API optimizes monitor creation
	// by deferring/batching it, making the timing non-deterministic from the test's perspective.
	// As a result, testing the blocking behavior of concurrent non-transactional writes is not
	// reliable in the fluent API without synchronous monitor creation guarantees.

	@Test
	public void txnWriteRead() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey4");

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

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));

		commitTxn(txn);

		rs = session.query(key).execute();
		record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));
	}

	@Test
	public void txnWriteAbort() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

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
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey6");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.delete(key)
			.withDurableDelete()
			.execute();

		commitTxn(txn);

		RecordStream rs = session.query(key).execute();
		if (rs.hasNext()) {
			Record record = rs.next().recordOrNull();
			assertNull(record);
		}
	}

	@Test
	public void txnDeleteAbort() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey7");

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
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnDeleteTwice() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey8");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.delete(key)
			.withDurableDelete()
			.execute();

		txnSession.delete(key)
			.withDurableDelete()
			.execute();

		commitTxn(txn);

		RecordStream rs = session.query(key).execute();
		if (rs.hasNext()) {
			Record record = rs.next().recordOrNull();
			assertNull(record);
		}
	}

	@Test
	public void txnTouch() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey9");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.touch(key).execute();

		commitTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnTouchAbort() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey10");

		session.upsert(key)
			.bins(binName)
			.values("val1")
			.execute();

		TransactionalSession txnSession = new TransactionalSession(cluster, Behavior.DEFAULT);
		Txn txn = txnSession.getCurrentTransaction();

		txnSession.touch(key).execute();

		abortTxn(txn);

		RecordStream rs = session.query(key).execute();
		Record record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnOperateWrite() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey11");

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

		Record record = rs.next().recordOrThrow();
		assertEquals("bal1", record.getString("bin2"));

		commitTxn(txn);

		rs = session.query(key).execute();
		record = rs.next().recordOrThrow();
		assertEquals("val2", record.getString(binName));
	}

	@Test
	public void txnOperateWriteAbort() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		Key key = args.set.id("mrtkey12");

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

		Record record = rs.next().recordOrThrow();
		assertEquals("bal1", record.getString("bin2"));

		abortTxn(txn);

		rs = session.query(key).execute();
		record = rs.next().recordOrThrow();
		assertEquals("val1", record.getString(binName));
	}

	@Test
	public void txnBatch() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

		java.util.List<Key> keys = args.set.ids(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

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

		commitTxn(txn);

		recs = session.query(keys).execute();
		assertBatchEqual(keys, recs, 2);
	}

	@Test
	public void txnBatchAbort() {
		assumeTrue(args.scMode, "Transactions require strong consistency namespaces");

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

	private void commitTxn(Txn txn) {
		TxnRoll tr = new TxnRoll(cluster, txn);
		Settings verifyPolicy = Behavior.DEFAULT.getSettings(
			Behavior.OpKind.SYSTEM_TXN_VERIFY,
			Behavior.OpShape.SYSTEM,
			Behavior.Mode.ANY
		);
		Settings rollPolicy = Behavior.DEFAULT.getSettings(
			Behavior.OpKind.SYSTEM_TXN_ROLL,
			Behavior.OpShape.SYSTEM,
			Behavior.Mode.ANY
		);

		switch (txn.getState()) {
			case OPEN:
				tr.verify(verifyPolicy, rollPolicy);
				tr.commit(rollPolicy);
				break;
			case VERIFIED:
				tr.commit(rollPolicy);
				break;
			case COMMITTED:
				break;
			case ABORTED:
				throw new AerospikeException(ResultCode.TXN_ALREADY_ABORTED, "Transaction already aborted");
		}
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
				throw new AerospikeException(ResultCode.TXN_ALREADY_COMMITTED, "Transaction already committed");
			case ABORTED:
				break;
		}
	}
}
