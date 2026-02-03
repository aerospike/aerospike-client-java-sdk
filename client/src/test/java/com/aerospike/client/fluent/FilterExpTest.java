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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.exp.Exp;

public class FilterExpTest extends ClusterTest {
	String binA = "A";
	String binB = "B";
	String binC = "C";
	String binD = "D";
	String binE = "E";

	String keyA = "A";
	byte[] keyB = new byte[] {(byte)'B'};
	String keyC = "C";

	@BeforeAll
	public static void register() {
		/* TODO Support UDF
		RegisterTask task = client.register(null,
				TestUDF.class.getClassLoader(), "udf/record_example.lua",
				"record_example.lua", Language.LUA);
		task.waitTillComplete();
		*/
	}

	@BeforeEach
	public void setUp() throws Exception {
        session.delete(args.set.ids(keyA, keyC)).execute();
        session.delete(args.set.id(keyB)).execute();

        session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(1)
	        .bin(binB).setTo(1.1)
	        .bin(binC).setTo("abcde")
	        .bin(binD).setTo(1)
	        .bin(binE).setTo(-1)
	        .execute();

	    session.upsert(args.set.id(keyB))
	    	.bin(binA).setTo(2)
	        .bin(binB).setTo(2.2)
	        .bin(binC).setTo("abcdeabcde")
	        .bin(binD).setTo(1)
	        .bin(binE).setTo(-2)
	        .execute();

	    session.upsert(args.set.id(keyC))
	    	.bin(binA).setTo(0)
	        .bin(binB).setTo(-1)
	        .bin(binC).setTo(1)
	        .execute();
	}

	@Test
	public void put() {
        session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(3)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

        RecordStream rs = session.query(args.set.id(keyA)).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(3, val);

        session.upsert(args.set.id(keyB))
	        .bin(binA).setTo(3)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

        rs = session.query(args.set.id(keyB)).execute();
        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getInt(binA);
		assertEquals(2, val);
	}

	@Test
	public void putExcept() {
        session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(3)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
	        .execute();

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
	        RecordStream rs = session.upsert(args.set.id(keyB))
		        .bin(binA).setTo(3)
		        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs.hasNext());
	        rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void get() {
        RecordStream rs = session.query(args.set.id(keyA))
    	    .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(1, val);

        rs = session.query(args.set.id(keyB))
    	    .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
        	.execute();

        assertFalse(rs.hasNext());
	}

	@Test
	public void getExcept() {
        RecordStream rs = session.query(args.set.id(keyA))
    	    .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
        	.execute();

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyB))
        	    .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
    	        .failOnFilteredOut()
            	.execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void batch() {
		List<Key> keys = args.set.ids(List.of(keyA, keyB, "doesn'Exist"));
        RecordStream rs = session.query(keys)
    	    .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(1, val);

		// Since "failOnFilteredOut" was specified, we expect there to be 2 record, as the second one was filtered out
        assertTrue(rs.hasNext());
        RecordResult recResult = rs.next();
        assertEquals(recResult.resultCode(), ResultCode.FILTERED_OUT);

        assertFalse(rs.hasNext());
	}

	@Test
	public void delete() {
        session.delete(args.set.id(keyA))
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    RecordStream rs = session.query(args.set.id(keyA)).execute();
	    assertFalse(rs.hasNext());

		rs = session.delete(args.set.id(keyB))
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    assertTrue(rs.hasNext());
	    rs.next().recordOrThrow();

	    Record rec;
	    int val;

	    rs = session.query(args.set.id(keyB)).execute();
	    assertTrue(rs.hasNext());
	    rec = rs.next().recordOrThrow();
	    val = rec.getInt(binA);
		assertEquals(2, val);
	}

	@Test
	public void deleteExcept() {
        session.delete(args.set.id(keyA))
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
	        .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
	        RecordStream rs = session.delete(args.set.id(keyB))
		        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		        .failOnFilteredOut()
		        .execute();

	        // TODO: hasNext() returns false so FILTERED_OUT exception is not thrown.
		    assertTrue(rs.hasNext());
		    rs.next().recordOrThrow();
        });

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void durableDelete() {
		Assumptions.assumeTrue(args.enterprise);

        session.delete(args.set.id(keyA))
        	.durablyDelete(true)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    RecordStream rs = session.query(args.set.id(keyA)).execute();
	    assertFalse(rs.hasNext());

        session.delete(args.set.id(keyB))
	    	.durablyDelete(true)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    rs = session.query(args.set.id(keyB)).execute();
	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(2, val);
	}

	@Test
	public void durableDeleteExcept() {
		Assumptions.assumeTrue(args.enterprise);

        session.delete(args.set.id(keyA))
	    	.durablyDelete(true)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
	        .execute();

        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
	        RecordStream rs = session.delete(args.set.id(keyB))
	        	.durablyDelete(true)
		        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		        .failOnFilteredOut()
		        .execute();

	        // TODO: hasNext() returns false so FILTERED_OUT exception is not thrown.
		    assertTrue(rs.hasNext());
		    rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

/* TODO Need read level operations support.
	@Test
	public void operateRead() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		Record r = client.operate(policy, keyA, Operation.get(binA));

		assertBinEqual(keyA, r, binA, 1);

		r = client.operate(policy, keyB, Operation.get(binA));

		assertEquals(null, r);
	}

	@Test
	public void operateReadExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.operate(policy, keyA, Operation.get(binA));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.operate(policy, keyB, Operation.get(binA));
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}
*/

	@Test
	public void operateWrite() {
		RecordStream rs = session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(3)
	        .get(binA)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    List<?> list = rec.getList(binA);
		assertEquals(2, list.size());
		int val = (int)(long)list.get(1);
		assertEquals(3, val);

		rs = session.upsert(args.set.id(keyB))
	        .bin(binA).setTo(3)
	        .get(binA)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

	    assertFalse(rs.hasNext());
	}

	@Test
	public void operateWriteExcept() {
		RecordStream rs = session.upsert(args.set.id(keyA))
	        .bin(binA).setTo(3)
	        .get(binA)
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    List<?> list = rec.getList(binA);
		assertEquals(2, list.size());
		int val = (int)(long)list.get(1);
		assertEquals(3, val);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.upsert(args.set.id(keyB))
		        .bin(binA).setTo(3)
		        .get(binA)
		        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

        assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

/* TODO Support UDF
	@Test
	public void udf() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		client.execute(policy, keyA, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		Record r = client.get(null, keyA);

		assertBinEqual(keyA, r, binA, 3);

		client.execute(policy, keyB, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		r = client.get(null, keyB);

		assertBinEqual(keyB, r, binA, 2);
	}

	@Test
	public void udfExcept() {
		WritePolicy policy = new WritePolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		policy.failOnFilteredOut = true;

		client.execute(policy, keyA, "record_example", "writeBin",
				Value.get(binA), Value.get(3));

		AerospikeException ae = assertThrows(AerospikeException.class, new ThrowingRunnable() {
			public void run() {
				client.execute(policy, keyB, "record_example", "writeBin",
					Value.get(binA), Value.get(3));
			}
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}
*/
	@Test
	public void filterExclusive() {
		/*
		Expression e = Exp.build(
			Exp.exclusive(
				Exp.eq(Exp.intBin(binA), Exp.val(1)),
				Exp.eq(Exp.intBin(binD), Exp.val(1))));
		*/
		testDsl("exclusive($.A == 1, $.D == 1)");
	}

	@Test
	public void filterAddInt() {
		/*
		Expression e = Exp.build(
			Exp.eq(
				Exp.add(Exp.intBin(binA), Exp.intBin(binD), Exp.val(1)),
				Exp.val(4)));
		*/
		testDsl("($.A + $.D + 1) == 4)");
	}

	@Test
	public void filterAddFloat() {
		/*
		String name = "x";
		Expression e = Exp.build(
			Exp.let(
				Exp.def(name, Exp.add(Exp.floatBin(binB), Exp.val(1.1))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(3.2999)),
					Exp.le(Exp.var(name), Exp.val(3.3001)))));
		*/
		testDsl("with(x = $.B.asFloat() + 1.1) do(${x} >= 3.2999 and ${x} <= 3.3001)");
	}

	@Test
	public void filterSub() {
		/*
		Expression e = Exp.build(
			Exp.eq(
				Exp.sub(Exp.val(1), Exp.intBin(binA), Exp.intBin(binD)),
			Exp.val(-2)));
		*/
		testDsl("(1 - $.A - $.D) == -2");
	}

	@Test
	public void filterMul() {
		/*
		Expression e = Exp.build(
			Exp.eq(
				Exp.mul(Exp.val(2), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4)));
		*/
		testDsl("(2 * $.A * $.D) == 4");
	}

	@Test
	public void filterDiv() {
		/*
		Expression e = Exp.build(
			Exp.eq(
				Exp.div(Exp.val(8), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4)));
		*/
		testDsl("(8 / $.A / $.D) == 4");
	}

	// TODO Replace Exp filter with DSL when pow() is supported.
	@Test
	public void filterPow() {
		String name = "x";
		Exp exp =
			Exp.let(
				Exp.def(name, Exp.pow(Exp.floatBin(binB), Exp.val(2.0))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(4.8399)),
					Exp.le(Exp.var(name), Exp.val(4.8401))));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when log() is supported.
	@Test
	public void filterLog() {
		String name = "x";
		Exp exp =
			Exp.let(
				Exp.def(name, Exp.log(Exp.floatBin(binB), Exp.val(2.0))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(1.1374)),
					Exp.le(Exp.var(name), Exp.val(1.1376))));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when mod() is supported.
	@Test
	public void filterMod() {
		Exp exp =
			Exp.eq(
				Exp.mod(Exp.intBin(binA), Exp.val(2)),
				Exp.val(0));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when abs() is supported.
	@Test
	public void filterAbs() {
		Exp exp =
			Exp.eq(
				Exp.abs(Exp.intBin(binE)),
				Exp.val(2));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when floor() is supported.
	@Test
	public void filterFloor() {
		Exp exp =
			Exp.eq(
				Exp.floor(Exp.floatBin(binB)),
				Exp.val(2.0));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when ceil() is supported.
	@Test
	public void filterCeil() {
		Exp exp =
			Exp.eq(
				Exp.ceil(Exp.floatBin(binB)),
				Exp.val(3.0));

		testExp(exp);
	}

	// TODO Replace Exp filter with DSL when int cast works.
	@Test
	public void filterToInt() {
		// $.B.asInt() == 2
		Exp exp =
			Exp.eq(
				Exp.toInt(Exp.floatBin(binB)),
				Exp.val(2));

		testExp(exp);
	}

	@Test
	public void filterToFloat() {
		Exp exp =
			Exp.eq(
				Exp.toFloat(Exp.intBin(binA)),
				Exp.val(2.0));

		testExp(exp);
		//testDsl("$.A.asFloat() == 2.0");
	}

	@Test
	public void filterIntAnd() {
		Exp exp1 =
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intAnd(Exp.intBin(binA), Exp.val(0)),
						Exp.val(0)),
					Exp.eq(
						Exp.intAnd(Exp.intBin(binA), Exp.val(0xFFFF)),
						Exp.val(1))));

		Exp exp2 =
			Exp.and(
				Exp.eq(
					Exp.intAnd(Exp.intBin(binA), Exp.val(0)),
					Exp.val(0)),
				Exp.eq(
					Exp.intAnd(Exp.intBin(binA), Exp.val(0xFFFF)),
					Exp.val(1)));

		testExps(exp1, exp2);
	}

	@Test
	public void filterIntOr() {
		Exp exp1 =
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intOr(Exp.intBin(binA), Exp.val(0)),
						Exp.val(1)),
					Exp.eq(
						Exp.intOr(Exp.intBin(binA), Exp.val(0xFF)),
						Exp.val(0xFF))));

		Exp exp2 =
			Exp.and(
				Exp.eq(
					Exp.intOr(Exp.intBin(binA), Exp.val(0)),
					Exp.val(1)),
				Exp.eq(
					Exp.intOr(Exp.intBin(binA), Exp.val(0xFF)),
					Exp.val(0xFF)));

		testExps(exp1, exp2);
	}

	@Test
	public void filterIntXor() {
		Exp exp1 =
			Exp.not(
				Exp.and(
					Exp.eq(
						Exp.intXor(Exp.intBin(binA), Exp.val(0)),
						Exp.val(1)),
					Exp.eq(
						Exp.intXor(Exp.intBin(binA), Exp.val(0xFF)),
						Exp.val(0xFE))));

		Exp exp2 =
			Exp.and(
				Exp.eq(
					Exp.intXor(Exp.intBin(binA), Exp.val(0)),
					Exp.val(1)),
				Exp.eq(
					Exp.intXor(Exp.intBin(binA), Exp.val(0xFF)),
					Exp.val(0xFE)));

		testExps(exp1, exp2);
	}

	@Test
	public void filterIntNot() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.intNot(Exp.intBin(binA)),
					Exp.val(-2)));

		Exp exp2 =
			Exp.eq(
				Exp.intNot(Exp.intBin(binA)),
				Exp.val(-2));

		testExps(exp1, exp2);
	}

	@Test
	public void filterLshift() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.lshift(Exp.intBin(binA), Exp.val(2)),
					Exp.val(4)));

		Exp exp2 =
			Exp.eq(
				Exp.lshift(Exp.intBin(binA), Exp.val(2)),
				Exp.val(4));

		testExps(exp1, exp2);
	}

	@Test
	public void filterRshift() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.rshift(Exp.intBin(binE), Exp.val(62)),
					Exp.val(3)));

		Exp exp2 =
			Exp.eq(
				Exp.rshift(Exp.intBin(binE), Exp.val(62)),
				Exp.val(3));

		testKeyBExps(exp1, exp2);
	}

	@Test
	public void filterARshift() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.arshift(Exp.intBin(binE), Exp.val(62)),
					Exp.val(-1)));

		Exp exp2 =
			Exp.eq(
				Exp.arshift(Exp.intBin(binE), Exp.val(62)),
				Exp.val(-1));

		testKeyBExps(exp1, exp2);
	}

	@Test
	public void filterBitCount() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.count(Exp.intBin(binA)),
					Exp.val(1)));

		Exp exp2 =
			Exp.eq(
				Exp.count(Exp.intBin(binA)),
				Exp.val(1));

		testExps(exp1, exp2);
	}

	@Test
	public void filterLscan() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.lscan(Exp.intBin(binA), Exp.val(true)),
					Exp.val(63)));

		Exp exp2 =
			Exp.eq(
				Exp.lscan(Exp.intBin(binA), Exp.val(true)),
				Exp.val(63));

		testExps(exp1, exp2);
	}

	@Test
	public void filterRscan() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.rscan(Exp.intBin(binA), Exp.val(true)),
					Exp.val(63)));

		Exp exp2 =
			Exp.eq(
				Exp.rscan(Exp.intBin(binA), Exp.val(true)),
				Exp.val(63));

		testExps(exp1, exp2);
	}

	@Test
	public void filterMin() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.min(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(-1)));

		Exp exp2 =
			Exp.eq(
				Exp.min(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
				Exp.val(-1));

		testExps(exp1, exp2);
	}

	@Test
	public void filterMax() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.max(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(1)));

		Exp exp2 =
			Exp.eq(
				Exp.max(Exp.intBin(binA), Exp.intBin(binD), Exp.intBin(binE)),
				Exp.val(1));

		testExps(exp1, exp2);
	}

	@Test
	public void filterCond() {
		Exp exp1 =
			Exp.not(
				Exp.eq(
					Exp.cond(
						Exp.eq(Exp.intBin(binA), Exp.val(0)), Exp.add(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.eq(Exp.intBin(binA), Exp.val(1)), Exp.sub(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.eq(Exp.intBin(binA), Exp.val(2)), Exp.mul(Exp.intBin(binD), Exp.intBin(binE)),
						Exp.val(-1)),
					Exp.val(2)));

		Exp exp2 =
			Exp.eq(
				Exp.cond(
					Exp.eq(Exp.intBin(binA), Exp.val(0)), Exp.add(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.eq(Exp.intBin(binA), Exp.val(1)), Exp.sub(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.eq(Exp.intBin(binA), Exp.val(2)), Exp.mul(Exp.intBin(binD), Exp.intBin(binE)),
					Exp.val(-1)),
				Exp.val(2));

		testExps(exp1, exp2);
	}

	@Test
	public void batchKeyFilter() {
		// Write/Delete records with filter.
		RecordStream rs = session
			.upsert(args.set.id(keyA))
				.bin(binA).setTo(3)
				.where("$.A == 1")
			.upsert(args.set.id(keyB))
				.bin(binA).setTo(3)
				.where("$.A == 1")
				.failOnFilteredOut()
			.delete(args.set.id(keyC))
				.where("$.A == 0")
			.execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();

	    assertTrue(rs.hasNext());
        RecordResult recResult = rs.next();
        assertEquals(recResult.resultCode(), ResultCode.FILTERED_OUT);

	    assertTrue(rs.hasNext());
	    rs.next().recordOrThrow();

	    assertFalse(rs.hasNext());

	    // Read records
		List<Key> keys = args.set.ids(List.of(keyA, keyB, keyC));

        rs = session.query(keys).execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(3, val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getInt(binA);
		assertEquals(2, val);

        assertFalse(rs.hasNext());
	}

	private void testDsl(String dsl) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyA))
		        .readingOnlyBins(binA)
		        .where(dsl)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyB))
	        .readingOnlyBins(binA)
	        .where(dsl)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(2, val);
	}

	private void testExp(Exp exp) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyA))
		        .readingOnlyBins(binA)
		        .where(exp)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyB))
	        .readingOnlyBins(binA)
	        .where(exp)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(2, val);
	}
/*
	private void testDsls(String dsl1, Exp dsl2) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyA))
		        .readingOnlyBins(binA)
		        .where(dsl1)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyA))
	        .readingOnlyBins(binA)
	        .where(dsl2)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(1, val);
	}
*/
	private void testExps(Exp exp1, Exp exp2) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyA))
		        .readingOnlyBins(binA)
		        .where(exp1)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyA))
	        .readingOnlyBins(binA)
	        .where(exp2)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(1, val);
	}

	private void testKeyBExps(Exp exp1, Exp exp2) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyB))
		        .where(exp1)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyB))
	        .where(exp2)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binE);
		assertEquals(-2, val);
	}
}
