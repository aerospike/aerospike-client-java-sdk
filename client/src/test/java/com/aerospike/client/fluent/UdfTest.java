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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.task.RegisterTask;

public class UdfTest extends ClusterTest {
    public static final String lua = """
		local function putBin(r,name,value)
		    if not aerospike:exists(r) then aerospike:create(r) end
		    r[name] = value
		    aerospike:update(r)
		end

		-- Set a particular bin
		function writeBin(r,name,value)
		    putBin(r,name,value)
		end

		-- Get a particular bin
		function readBin(r,name)
		    return r[name]
		end

		-- Return generation count of record
		function getGeneration(r)
		    return record.gen(r)
		end

		-- Update record only if gen hasn't changed
		function writeIfGenerationNotChanged(r,name,value,gen)
		    if record.gen(r) == gen then
		        r[name] = value
		        aerospike:update(r)
		    end
		end

		-- Set a particular bin only if record does not already exist.
		function writeUnique(r,name,value)
		    if not aerospike:exists(r) then
		        aerospike:create(r)
		        r[name] = value
		        aerospike:update(r)
		    end
		end

		-- Validate value before writing.
		function writeWithValidation(r,name,value)
		    if (value >= 1 and value <= 10) then
		        putBin(r,name,value)
		    else
		        error("1000:Invalid value")
		    end
		end

		-- Record contains two integer bins, name1 and name2.
		-- For name1 even integers, add value to existing name1 bin.
		-- For name1 integers with a multiple of 5, delete name2 bin.
		-- For name1 integers with a multiple of 9, delete record.
		function processRecord(r,name1,name2,addValue)
		    local v = r[name1]

		    if (v % 9 == 0) then
		        aerospike:remove(r)
		        return
		    end

		    if (v % 5 == 0) then
		        r[name2] = nil
		        aerospike:update(r)
		        return
		    end

		    if (v % 2 == 0) then
		        r[name1] = v + addValue
		        aerospike:update(r)
		    end
		end

		-- Append to end of regular list bin
		function appendListBin(r, binname, value)
		  local l = r[binname]

		  if l == nil then
		    l = list()
		  end

		  list.append(l, value)
		  r[binname] = l
		  aerospike:update(r)
		end

		-- Set expiration of record
		-- function expire(r,ttl)
		--    if record.ttl(r) == gen then
		--        r[name] = value
		--        aerospike:update(r)
		--    end
		-- end
		""";

	@BeforeAll
	public static void register() {
		RegisterTask task = session.registerUdfString(lua, "record_example.lua");
		task.waitTillComplete();
	}

	@Test
	public void writeUsingUdf() {
		Key key = args.set.id("writeUsingUdf");
		String binName = "udfbin1";

		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "writeBin")
        	.passing(binName, "string value")
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.getFirstRecord();
        assertNull(rec);

        rs = session.query(key)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
    	String val = rec.getString(binName);
		assertEquals("string value", val);
	}

	@Test
	public void writeIfGenerationNotChanged() {
		Key key = args.set.id("writeIfGenerationNotChanged");
		String binName = "udfbin2";

		// Seed record.
        session.upsert(key)
	    	.bin(binName).append("string value")
	        .execute();

		// Get record generation.
		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "getGeneration")
        	.passing(binName, "string value")
        	.execute();

        assertTrue(rs.hasNext());
        Optional<Object> obj = rs.getFirstUdfResult();
        int gen = (int)(long)obj.orElseThrow();

		// Write record if generation has not changed.
		rs = session.executeUdf(key)
        	.function("record_example", "writeIfGenerationNotChanged")
        	.passing(binName, "string value", gen)
        	.execute();

        assertTrue(rs.hasNext());
        obj = rs.getFirstUdfResult();
        assertTrue(obj.isEmpty());
	}

	@Test
	public void writeIfNotExists() {
		Key key = args.set.id("writeIfNotExists");
		String binName = "udfbin3";

		// Delete record if it already exists.
        session.delete(key).execute();

		// Write record only if not already exists. This should succeed.
		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "writeUnique")
        	.passing(binName, "first")
        	.execute();

		// Verify record written.
        rs = session.query(key)
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
    	String val = rec.getString(binName);
		assertEquals("first", val);

		// Write record second time. This should fail.
		rs = session.executeUdf(key)
        	.function("record_example", "writeUnique")
        	.passing(binName, "second")
        	.execute();

		// Verify record not written.
        rs = session.query(key)
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
    	val = rec.getString(binName);
		assertEquals("first", val);
	}

	@Test
	public void writeWithValidation() {
		Key key = args.set.id("writeWithValidation");
		String binName = "udfbin4";

		// Lua function writeWithValidation accepts number between 1 and 10.
		// Write record with valid value.
		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "writeWithValidation")
        	.passing(binName, 4)
        	.execute();

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

        // Write record with invalid value.
		AerospikeException ae = assertThrows(AerospikeException.class, () -> {
			RecordStream rs2 = session.executeUdf(key)
	        	.function("record_example", "writeWithValidation")
	        	.passing(binName, 11)
	        	.execute();

	        assertTrue(rs2.hasNext());
	        rs2.next().recordOrThrow();
		});

		// The UDF 1000 error code is returned by the writeWithValidation() lua function.
		assertEquals(1000, ae.getResultCode());
	}

	@Test
	public void writeListMapUsingUdf() {
		Key key = args.set.id("writeListMapUsingUdf");
		String binName = "udfbin5";

		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(8L);

		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1L);
		innerMap.put(2L, "b");
		innerMap.put("list", inner);

		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(4L);
		list.add(inner);
		list.add(innerMap);

		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "writeBin")
        	.passing(binName, list)
        	.execute();

        assertTrue(rs.hasNext());
        rs.next().recordOrThrow();

		rs = session.executeUdf(key)
        	.function("record_example", "readBin")
        	.passing(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Optional<Object> obj = rs.getFirstUdfResult();
        List<?> received = (List<?>)obj.orElseThrow();
		assertEquals(list, received);

		String value = "appended value";

		rs = session.executeUdf(key)
        	.function("record_example", "appendListBin")
        	.passing(binName, value)
        	.execute();

        assertTrue(rs.hasNext());
        rs.getFirstUdfResult();

        rs = session.query(key)
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        Object receivedObject = rec.getValue(binName);
		assertNotNull(receivedObject);
		assertTrue(receivedObject instanceof List<?>);

		List<?> receivedList = (List<?>)receivedObject;
		assertEquals(5, receivedList.size());
		assertEquals(value, receivedList.get(4));
	}

	@Test
	public void writeBlobUsingUdf() {
		Key key = args.set.id("writeBlobUsingUdf");
		String binName = "udfbin6";

		// Create packed blob using standard java tools.
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			dos.writeInt(9845);
			dos.writeUTF("Hello world.");
		}
		catch (IOException e) {
			throw new RuntimeException("DataOutputStream error: " + e.getMessage());
		}

		byte[] blob = baos.toByteArray();

		RecordStream rs = session.executeUdf(key)
        	.function("record_example", "writeBin")
        	.passing(binName, blob)
        	.execute();

        assertTrue(rs.hasNext());
        rs.getFirstUdfResult();

		rs = session.executeUdf(key)
        	.function("record_example", "readBin")
        	.passing(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Optional<Object> obj = rs.getFirstUdfResult();
        byte[] received = (byte[])obj.orElseThrow();
		assertArrayEquals(blob, received);
	}

	@Test
	public void batchUDF() {
		Key key1 = args.set.id(20000);
		Key key2 = args.set.id(20001);
		String binName = "B5";

		List<Key> keys = List.of(key1, key2);

        session.delete(keys).execute();

		RecordStream rs = session.executeUdf(keys)
        	.function("record_example", "writeBin")
        	.passing(binName, "value5")
        	.execute();

        assertTrue(rs.hasNext());
        rs.getFirstUdfResult();

        rs = session.query(keys)
        	.readingOnlyBins(binName)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        String val = rec.getString(binName);
		assertEquals("value5", val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        val = rec.getString(binName);
		assertEquals("value5", val);

        assertFalse(rs.hasNext());
	}

	@Test
	public void batchUDFError() {
		Key key1 = args.set.id(20002);
		Key key2 = args.set.id(20004);
		String binName = "B5";

		List<Key> keys = List.of(key1, key2);

        session.delete(keys).execute();

		RecordStream rs = session.executeUdf(keys)
        	.function("record_example", "writeWithValidation")
        	.passing(binName, 999)
        	.includeMissingKeys()
        	.execute();

        assertTrue(rs.hasNext());
        RecordResult res = rs.next();
		assertEquals(ResultCode.UDF_BAD_RESPONSE, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.UDF_BAD_RESPONSE, res.resultCode());

        assertFalse(rs.hasNext());
	}

	@Test
	public void batchUDFComplex() {
		Key key1 = args.set.id(20005);
		Key key2 = args.set.id(20007);
		String binName = "B5";

		RecordStream rs = session
			.executeUdf(key1)
        		.function("record_example", "writeBin")
	        	.passing(binName, "value1")
			.executeUdf(key2)
        		.function("record_example", "writeWithValidation")
	        	.passing(binName, 5)
			.executeUdf(key2)
        		.function("record_example", "writeWithValidation")
	        	.passing(binName, 999)
	        .execute();

        assertTrue(rs.hasNext());
        RecordResult res = rs.next();
		assertEquals(ResultCode.OK, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.OK, res.resultCode());

        assertTrue(rs.hasNext());
        res = rs.next();
		assertEquals(ResultCode.UDF_BAD_RESPONSE, res.resultCode());

        assertFalse(rs.hasNext());

		List<Key> keys = List.of(key1, key2);

		rs = session.query(keys)
        	.execute();

        assertTrue(rs.hasNext());
        Record rec = rs.next().recordOrThrow();
        String val = rec.getString(binName);
		assertEquals("value1", val);

        assertTrue(rs.hasNext());
        rec = rs.next().recordOrThrow();
        int ival = rec.getInt(binName);
		assertEquals(5, ival);

        assertFalse(rs.hasNext());
	}
}
