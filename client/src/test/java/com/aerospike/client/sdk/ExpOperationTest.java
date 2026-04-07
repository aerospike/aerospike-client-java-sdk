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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;

public class ExpOperationTest extends ClusterTest {
	String binA = "A";
	String binB = "B";
	String binC = "C";
	String binD = "D";
	String binH = "H";
	String expVar = "EV";

	String keyA = "A";
	byte[] keyB = new byte[] {(byte)'B'};

	@BeforeEach
	public void setUp() throws Exception {
        session.delete(args.set.id(keyA)).execute();
        session.delete(args.set.id(keyB)).execute();

        session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(1)
	        .bin(binD).setTo(2)
	        .execute();

        session.upsert(args.set.id(keyB))
	        .bin(binB).setTo(2)
	        .bin(binD).setTo(2)
	        .execute();
	}

	@Test
	public void expReadEvalError() {
		String ael = "$.A + 4";

        RecordStream rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(expVar);
		assertEquals(5, val);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			// binA doesn't exist on keyB.
	        RecordStream rs2 = session.query(args.set.id(keyB))
            	.bin(expVar).selectFrom(ael)
            	.execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Try NO_FAIL.
        rs = session.query(args.set.id(keyB))
        	.bin(expVar).selectFrom(ael, arg -> arg.ignoreEvalFailure())
        	.execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        Object obj = rec.getValue(expVar);
		assertNull(obj);
	}

	@Test
	public void expReadOnWriteEvalError() {
		String wael = "$.D";
		String rael = "$.A";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binD).upsertFrom(wael)
	        .bin(expVar).selectFrom(rael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(expVar);
		assertEquals(1, val);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.update(args.set.id(keyB))
	        	.bin(binD).upsertFrom(wael)
		        .bin(expVar).selectFrom(rael)
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Try NO_FAIL.
		rs = session.update(args.set.id(keyB))
	        .bin(expVar).selectFrom(rael, arg -> arg.ignoreEvalFailure())
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        Object obj = rec.getValue(expVar);
		assertNull(obj);
	}

	@Test
	public void expWriteEvalError() {
		String wael = "$.A + 4";
		String rael = "$.C + 1";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(wael)
	        .bin(expVar).selectFrom(rael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(expVar);
		assertEquals(6, val);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.update(args.set.id(keyB))
	        	.bin(binC).upsertFrom(wael)
		        .bin(expVar).selectFrom(rael)
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		// Try NO_FAIL.
		rs = session.update(args.set.id(keyB))
	        .bin(binC).upsertFrom(wael, arg -> arg.ignoreEvalFailure())
	        .bin(expVar).selectFrom(rael, arg -> arg.ignoreEvalFailure())
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        Object obj = rec.getValue(expVar);
		assertNull(obj);
	}

	@Test
	public void expWritePolicyError() {
		Key key = args.set.id(keyA);
		String wael = "$.A + 4";

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs = session.update(key)
	        	.bin(binC).updateFrom(wael)
		        .execute();

	        assertTrue(rs.hasNext());
	        rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());

		RecordStream rs = session.update(key)
        	.bin(binC).updateFrom(wael, arg -> arg.ignoreOpFailure())
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		Object val = rec.getValue(binC);
		assertEquals(null, val);

		rs = session.update(key)
        	.bin(binC).insertFrom(wael)
	        .execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getValue(binC);
		assertEquals(null, val);

		ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.update(key)
	        	.bin(binC).insertFrom(wael)
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.BIN_EXISTS_ERROR, ae.getResultCode());

		rs = session.update(key)
        	.bin(binC).insertFrom(wael, arg -> arg.ignoreOpFailure())
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getValue(binC);
		assertEquals(null, val);

		Expression nilael = Exp.build(Exp.nil());
		// TODO How specify nil in AEL?
		//String nilael = "nil";

		ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.update(key)
				.bin(binC).upsertFrom(nilael)
			    .execute();

			assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		rs = session.update(key)
        	.bin(binC).upsertFrom(nilael, arg -> arg.ignoreOpFailure())
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getValue(binC);
		assertEquals(null, val);

		rs = session.update(key)
        	.bin(binC).upsertFrom(nilael, arg -> arg.ignoreOpFailure().deleteIfNull())
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getValue(binC);
		assertEquals(null, val);

		rs = session.update(key)
        	.bin(binC).insertFrom(wael)
	        .execute();

		assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getValue(binC);
		assertEquals(null, val);
	}

	@Test
	public void expReturnsUnknown() {
		Expression ael = Exp.build(
			Exp.cond(
				Exp.eq(Exp.intBin(binC), Exp.val(5)), Exp.unknown(),
				Exp.binExists(binA), Exp.val(5),
				Exp.unknown()));

		// TODO: Convert from Expression to AEL String.
		//String ael = "when ($.C == 5 => unknown, $.A.exists() => 5, default => unknown)";

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs = session.update(args.set.id(keyA))
	        	.bin(binC).upsertFrom(ael)
	        	.bin(binC).get()
		        .execute();

	        assertTrue(rs.hasNext());
	        rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael, arg -> arg.ignoreEvalFailure())
        	.bin(binC).get()
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		List<?> results = rec.getList(binC);
		Object val = results.get(0);
		assertEquals(null, val);
		val = results.get(1);
		assertEquals(null, val);
	}

	@Test
	public void expReturnsNil() {
		Expression ael = Exp.build(Exp.nil());
		// TODO: Convert from Expression to AEL String.
		//String ael = "nil";

		RecordStream rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.bin(binC).get()
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		Object val = rec.getValue(expVar);
		assertEquals(null, val);
	}

	@Test
	public void expReturnsInt() {
		String ael = "$.A + 4";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> results = rec.getList(binC);
		Object obj = results.get(0);
		assertEquals(null, obj);
		int val = (int)(long)results.get(1);
		assertEquals(5, val);
		val = rec.getInt(expVar);
		assertEquals(5, val);

        rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getInt(expVar);
		assertEquals(5, val);
	}

	@Test
	public void expReturnsFloat() {
		//Expression ael = Exp.build(Exp.add(Exp.toFloat(Exp.intBin(binA)), Exp.val(4.0)));
		String ael = "$." + binA + ".asFloat() + 4.0";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> results = rec.getList(binC);
		double val = (Double)results.get(1);
		double delta = 0.000001;
		assertEquals(5.0, val, delta);

		val = rec.getDouble(expVar);
		assertEquals(5.0, val, delta);

        rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getDouble(expVar);
		assertEquals(5.0, val, delta);
	}

	@Test
	public void expReturnsString() {
		String str = "xxx";
		String ael = "'xxx'";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> results = rec.getList(binC);
		String val = (String)results.get(1);
		assertEquals(str, val);

		val = rec.getString(expVar);
		assertEquals(str, val);

        rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
		val = rec.getString(expVar);
		assertEquals(str, val);
	}

	@Test
	public void expReturnsBlob() {
		byte[] bytes = new byte[] {0x78, 0x78, 0x78};
		//Expression ael = Exp.build(Exp.val(bytes));
		String ael = "x'" + Buffer.bytesToHexString(bytes) + "'";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> results = rec.getList(binC);
		byte[] val = (byte[])results.get(1);
		String resultString = "bytes not equal";
		assertArrayEquals(bytes, val, resultString);

		val = (byte[])rec.getValue(expVar);
		assertArrayEquals(bytes, val, resultString);

        rs = session.query(args.set.id(keyA))
        	.bin(expVar).selectFrom(ael)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = (byte[])rec.getValue(expVar);
		assertArrayEquals(bytes, val, resultString);
	}

	@Test
	public void expReturnsBoolean() {
		String ael = "$.A == 1";

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
		List<?> results = rec.getList(binC);
		boolean val = (Boolean)results.get(1);
		assertTrue(val);

		val = rec.getBoolean(expVar);
		assertTrue(val);
	}

	@Test
	public void expReturnsHLL() {
		// TODO: Support HLL.
		/*
		Expression ael = Exp.build(HLLExp.init(HLLPolicy.Default, Exp.val(4), Exp.nil()));

		RecordStream rs = session.update(args.set.id(keyA))
        	.bin(binC).upsertFrom(ael)
        	.bin(binC).get()
	        .bin(expVar).selectFrom(ael)
	        .execute();

		Record record = client.operate(null, keyA,
			HLLOperation.init(HLLPolicy.Default, binH, 4),
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binH),
			Operation.get(binC),
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		assertRecordFound(keyA, record);
		//System.out.println(record);

		List<?> results = record.getList(binH);
		HLLValue valH = (HLLValue)results.get(1);

		results = record.getList(binC);
		HLLValue valC = (HLLValue)results.get(1);

		HLLValue valExp = record.getHLLValue(expVar);

		String resultString = "bytes not equal";
		assertArrayEquals(resultString, valH.getBytes(), valC.getBytes());
		assertArrayEquals(resultString, valH.getBytes(), valExp.getBytes());

		record = client.operate(null, keyA,
			ExpOperation.read(expVar, exp, ExpReadFlags.DEFAULT)
			);

		valExp = record.getHLLValue(expVar);
		assertArrayEquals(resultString, valH.getBytes(), valExp.getBytes());
		*/
	}

	@Test
	public void expMerge() {
		//Expression e = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(0)));
		//Expression eand = Exp.build(Exp.and(Exp.expr(e), Exp.eq(Exp.intBin(binD), Exp.val(2))));
		//Expression eor = Exp.build(Exp.or(Exp.expr(e), Exp.eq(Exp.intBin(binD), Exp.val(2))));
		String e = "$.A == 0";
		String eand = e + " and $.D == 2";
		String eor = e + " or $.D == 2";

		RecordStream rs = session.query(args.set.id(keyA))
	        .bin("res1").selectFrom(eand)
	        .bin("res2").selectFrom(eor)
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();

		boolean res1 = rec.getBoolean("res1");
		assertFalse(res1);

		boolean res2 = rec.getBoolean("res2");
		assertTrue(res2);
	}
}
