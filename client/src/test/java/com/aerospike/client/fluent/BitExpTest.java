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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.exp.BitExp;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.operation.BitOverflowAction;
import com.aerospike.client.fluent.operation.BitPolicy;

public class BitExpTest extends ClusterTest {
	private static final String binA = "A";

	@Test
	public void callRead() {
		String key = "callRead";
		byte[] bytes = new byte[] {0x01, 0x42, 0x03, 0x04, 0x05};

        session.delete(args.set.id(key)).execute();

        session.upsert(args.set.id(key))
	        .bin(binA).setTo(bytes)
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
        byte[] val = r.getBytes(binA);
        assertArrayEquals(bytes, val);

		get(key);
		count(key);
		lscan(key);
		rscan(key);
		getInt(key);
	}

	@Test
	public void callModify() {
		String key = "callModify";
		byte[] bytes = new byte[] {0x01, 0x42, 0x03, 0x04, 0x05};

        session.delete(args.set.id(key)).execute();

        session.upsert(args.set.id(key))
	        .bin(binA).setTo(bytes)
	        .execute();

        RecordStream rs = session.query(args.set.id(key))
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
        byte[] val = r.getBytes(binA);
        assertArrayEquals(bytes, val);

        resize(key);
		insert(key);
		remove(key);
		set(key);
		or(key);
		xor(key);
		and(key);
		not(key);
		lshift(key);
		rshift(key);
		add(key);
		subtract(key);
		setInt(key);
	}

	private void get(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

		rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				Exp.val(new byte[] {0x03})));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void count(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.count(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.count(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.count(Exp.val(16), Exp.val(8), Exp.blobBin(binA)),
				BitExp.count(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void lscan(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.lscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(5)));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.ne(
				BitExp.lscan(Exp.val(0), Exp.val(8), Exp.val(true),
					BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))),
				Exp.val(5)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.lscan(Exp.val(0), Exp.val(8), Exp.val(true),
					BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))),
				Exp.val(5)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);

		filterExp = Exp.build(
			Exp.eq(
				BitExp.lscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(5)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void rscan(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(7)));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.rscan(Exp.val(32), Exp.val(8), Exp.val(true), Exp.blobBin(binA)),
				Exp.val(7)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void getInt(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(32), Exp.val(8), true, Exp.blobBin(binA)),
				Exp.val(0x05)));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(32), Exp.val(8), true, Exp.blobBin(binA)),
				Exp.val(0x05)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void resize(String key) {
		Exp size = Exp.val(6);

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA)),
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA)),
				BitExp.resize(BitPolicy.Default, size, 0, Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void insert(String key) {
		byte[] bytes = new byte[] {(byte)0xff};
		int expected = 0xff;

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(8), Exp.val(8), false,
					BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				Exp.val(expected)));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(8), Exp.val(8), false,
					BitExp.insert(BitPolicy.Default, Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				Exp.val(expected)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void remove(String key) {
		int expected = 0x42;

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.getInt(Exp.val(0), Exp.val(8), false,
					BitExp.remove(BitPolicy.Default, Exp.val(0), Exp.val(1), Exp.blobBin(binA))),
				Exp.val(expected)));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.getInt(Exp.val(0), Exp.val(8), false,
					BitExp.remove(BitPolicy.Default, Exp.val(0), Exp.val(1), Exp.blobBin(binA))),
				Exp.val(expected)));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void set(String key) {
		byte[] bytes = new byte[] {(byte)0x80};

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.set(BitPolicy.Default, Exp.val(31), Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.set(BitPolicy.Default, Exp.val(31), Exp.val(1), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void or(String key) {
		byte[] bytes = new byte[] {(byte)0x01};

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.or(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.or(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(32), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void xor(String key) {
		byte[] bytes = new byte[] {(byte)0x02};

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.xor(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.xor(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void and(String key) {
		byte[] bytes = new byte[] {(byte)0x01};

		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.and(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(0), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.and(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(bytes), Exp.blobBin(binA))),
				BitExp.get(Exp.val(0), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void not(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.not(BitPolicy.Default, Exp.val(6), Exp.val(1), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(8),
					BitExp.not(BitPolicy.Default, Exp.val(6), Exp.val(1), Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void lshift(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(0), Exp.val(6),
					BitExp.lshift(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(2), Exp.val(6), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(0), Exp.val(6),
					BitExp.lshift(BitPolicy.Default, Exp.val(0), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(2), Exp.val(6), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void rshift(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(26), Exp.val(6),
					BitExp.rshift(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(6), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(26), Exp.val(6),
					BitExp.rshift(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(2), Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(6), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void add(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(16), Exp.val(8),
					BitExp.add(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(16), Exp.val(8),
					BitExp.add(BitPolicy.Default, Exp.val(16), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(24), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void subtract(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.subtract(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.subtract(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(1), false, BitOverflowAction.FAIL, Exp.blobBin(binA))),
				BitExp.get(Exp.val(16), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}

	private void setInt(String key) {
		Expression filterExp = Exp.build(
			Exp.ne(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.setInt(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(0x42), Exp.blobBin(binA))),
				BitExp.get(Exp.val(8), Exp.val(8), Exp.blobBin(binA))));

        RecordStream rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertFalse(rs.hasNext());

		filterExp = Exp.build(
			Exp.eq(
				BitExp.get(Exp.val(24), Exp.val(8),
					BitExp.setInt(BitPolicy.Default, Exp.val(24), Exp.val(8), Exp.val(0x42), Exp.blobBin(binA))),
				BitExp.get(Exp.val(8), Exp.val(8), Exp.blobBin(binA))));

        rs = session.query(args.set.id(key))
            .where(filterExp)
        	.execute();

        assertTrue(rs.hasNext());
        Record r = rs.next().recordOrThrow();
		assertNotNull(r);
	}
}
