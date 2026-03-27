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
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.task.RegisterTask;

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
		RegisterTask task = session.registerUdfString(UdfTest.lua, "record_example.lua");
		task.waitTillComplete();
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

		    assertTrue(rs.hasNext());
		    rs.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void operateRead() {
		RecordStream rs = session.upsert(args.set.id(keyA))
        	.bin(binA).get()
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(1, val);


		rs = session.upsert(args.set.id(keyB))
        	.bin(binA).get()
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .execute();

        assertFalse(rs.hasNext());
	}

	@Test
	public void operateReadExcept() {
		RecordStream rs = session.upsert(args.set.id(keyA))
        	.bin(binA).get()
	        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
	        .failOnFilteredOut()
	        .execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
		assertEquals(1, val);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
    		RecordStream rs2 = session.upsert(args.set.id(keyB))
	        	.bin(binA).get()
		        .where("$.A == 1") // Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));
		        .failOnFilteredOut()
		        .execute();

		    assertTrue(rs2.hasNext());
		    rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

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

	@Test
	public void udf() {
		Key key1 = args.set.id(keyA);
		Expression filter = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		session.executeUdf(key1)
        	.function("record_example", "writeBin")
        	.passing(binA, 3)
        	.where(filter)
        	.execute();

		RecordStream rs = session.query(key1).execute();
        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        int val = rec.getInt(binA);
        assertEquals(3, val);

		Key key2 = args.set.id(keyB);

		session.executeUdf(key2)
	    	.function("record_example", "writeBin")
	    	.passing(binA, 3)
	    	.where(filter)
	    	.execute();

		rs = session.query(key2).execute();
        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getInt(binA);
        assertEquals(2, val);
	}

	@Test
	public void udfExcept() {
		Key key1 = args.set.id(keyA);
		Expression filter = Exp.build(Exp.eq(Exp.intBin(binA), Exp.val(1)));

		session.executeUdf(key1)
	    	.function("record_example", "writeBin")
	    	.passing(binA, 3)
	    	.where(filter)
	    	.failOnFilteredOut()
	    	.execute();

		Key key2 = args.set.id(keyB);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.executeUdf(key2)
		    	.function("record_example", "writeBin")
		    	.passing(binA, 3)
		    	.where(filter)
		    	.failOnFilteredOut()
		    	.execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
	}

	@Test
	public void filterExclusive() {
		Exp exp =
			Exp.exclusive(
				Exp.eq(Exp.intBin(binA), Exp.val(1)),
				Exp.eq(Exp.intBin(binD), Exp.val(1)));

		testExp(exp);
		testDsl("exclusive($.A == 1, $.D == 1)");
	}

	@Test
	public void filterAddInt() {
		Exp exp =
			Exp.eq(
				Exp.add(Exp.intBin(binA), Exp.intBin(binD), Exp.val(1)),
				Exp.val(4));

		testExp(exp);
		testDsl("($.A + $.D + 1) == 4)");
	}

	@Test
	public void filterAddFloat() {
		String name = "x";
		Exp exp =
			Exp.let(
				Exp.def(name, Exp.add(Exp.floatBin(binB), Exp.val(1.1))),
				Exp.and(
					Exp.ge(Exp.var(name), Exp.val(3.2999)),
					Exp.le(Exp.var(name), Exp.val(3.3001))));

		testExp(exp);
		testDsl("let(x = $.B.asFloat() + 1.1) then(${x} >= 3.2999 and ${x} <= 3.3001)");
	}

	@Test
	public void filterSub() {
		Exp exp =
			Exp.eq(
				Exp.sub(Exp.val(1), Exp.intBin(binA), Exp.intBin(binD)),
			Exp.val(-2));

		testExp(exp);
		testDsl("(1 - $.A - $.D) == -2");
	}

	@Test
	public void filterMul() {
		Exp exp =
			Exp.eq(
				Exp.mul(Exp.val(2), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4));

		testExp(exp);
		testDsl("(2 * $.A * $.D) == 4");
	}

	@Test
	public void filterDiv() {
		Exp exp =
			Exp.eq(
				Exp.div(Exp.val(8), Exp.intBin(binA), Exp.intBin(binD)),
				Exp.val(4));

		testExp(exp);
		testDsl("(8 / $.A / $.D) == 4");
	}

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
		testDsl("let (x = $." + binB + ".asFloat() ** 2.0) then (${x} >= 4.8399 and ${x} <= 4.8401)");
	}

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
		testDsl("let (x = log($." + binB + ".asFloat(), 2.0)) then (${x} >= 1.1374 and ${x} <= 1.1376)");
	}

	@Test
	public void filterMod() {
		Exp exp =
			Exp.eq(
				Exp.mod(Exp.intBin(binA), Exp.val(2)),
				Exp.val(0));

		testExp(exp);
		testDsl("($." + binA + " % 2) == 0");
	}

	@Test
	public void filterAbs() {
		Exp exp =
			Exp.eq(
				Exp.abs(Exp.intBin(binE)),
				Exp.val(2));

		testExp(exp);
		testDsl("abs($." + binE + ") == 2");
	}

	@Test
	public void filterFloor() {
		Exp exp =
			Exp.eq(
				Exp.floor(Exp.floatBin(binB)),
				Exp.val(2.0));

		testExp(exp);
		testDsl("floor($." + binB + ".asFloat()) == 2.0");
	}

	@Test
	public void filterCeil() {
		Exp exp =
			Exp.eq(
				Exp.ceil(Exp.floatBin(binB)),
				Exp.val(3.0));

		testExp(exp);
		testDsl("ceil($." + binB + ".asFloat()) == 3.0");
	}

	// TODO Replace Exp filter with DSL when int cast works.
	@Test
	public void filterToInt() {
		Exp exp =
			Exp.eq(
				Exp.toInt(Exp.floatBin(binB)),
				Exp.val(2));

		testExp(exp);
		//testDsl("$." + binB + ".asInt()) == 2");
	}

	// TODO Replace Exp filter with DSL when float cast works.
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
		testDsls("(($." + binA + " & 0) == 0) and ($." + binA + " & 0xFFFF) == 1");
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
		testDsls("(($." + binA + " | 0) == 1) and ($." + binA + " | 0xFF) == 0xFF");
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
		testDsls("(($." + binA + " ^ 0) == 1) and ($." + binA + " ^ 0xFF) == 0xFE");
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
		testDsls("~($." + binA + ") == -2");
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
		testDsls("$." + binA + " << 2 == 4");
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
		testKeyBDsls("$." + binE + " >>> 62 == 3");
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
		testKeyBDsls("$." + binE + " >> 62 == -1");
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
		testDsls("countOneBits($." + binA + ") == 1");
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
		testDsls("findBitLeft($." + binA + ", true) == 63");
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
		testDsls("findBitRight($." + binA + ", true) == 63");
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
		testDsls("min($." + binA + ", $." + binD + ", $." + binE + ") == -1");
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
		testDsls("max($." + binA + ", $." + binD + ", $." + binE + ") == 1");
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
		// TODO: This results in a PARAMETER_ERROR. Needs to be fixed.
		/*
		testDsls("when($." + binA + " == 0 => $." + binD + " + $." + binE +
			        ", $." + binA + " == 1 => $." + binD + " - $." + binE +
					", $." + binA + " == 2 => $." + binD + " * $." + binE +
					", default => -1) == 2");
		*/
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

	private void testDsls(String dsl) {
		String dslNot = "not(" + dsl + ")";

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyA))
		        .readingOnlyBins(binA)
		        .where(dslNot)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyA))
	        .readingOnlyBins(binA)
	        .where(dsl)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binA);
		assertEquals(1, val);
	}

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

	private void testKeyBDsls(String dsl) {
		String dslNot = "not(" + dsl + ")";

		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.query(args.set.id(keyB))
		        .where(dslNot)
		        .failOnFilteredOut()
		        .execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());

		RecordStream rs = session.query(args.set.id(keyB))
	        .where(dsl)
	        .failOnFilteredOut()
	        .execute();

	    assertTrue(rs.hasNext());
	    Record rec = rs.next().recordOrThrow();
	    int val = rec.getInt(binE);
		assertEquals(-2, val);
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
