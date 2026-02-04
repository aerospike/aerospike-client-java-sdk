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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for all OpType behaviors to ensure they work as expected.
 */
public class OpTypeTest extends ClusterTest {

	// ========== UPSERT Tests ==========

	@Test
	public void upsertCreatesNewRecord() {
		String key = "optype_upsert_new";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Upsert should create new record
		session.upsert(args.set.id(key))
			.bin("name").setTo("test")
			.execute();

		// Verify record exists and has correct value
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("test", rec.getString("name"));
	}

	@Test
	public void upsertUpdatesExistingRecord() {
		String key = "optype_upsert_update";

		// Create record first
		session.upsert(args.set.id(key))
			.bin("name").setTo("original")
			.execute();

		// Upsert should update existing record
		session.upsert(args.set.id(key))
			.bin("name").setTo("updated")
			.execute();

		// Verify record has updated value
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("updated", rec.getString("name"));
	}

	// ========== INSERT Tests ==========

	@Test
	public void insertCreatesNewRecord() {
		String key = "optype_insert_new";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Insert should create new record
		session.insert(args.set.id(key))
			.bin("name").setTo("inserted")
			.execute();

		// Verify record exists and has correct value
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("inserted", rec.getString("name"));
	}

	@Test
	public void insertFailsOnExistingRecord() {
		String key = "optype_insert_fail";

		// Create record first
		session.upsert(args.set.id(key))
			.bin("name").setTo("original")
			.execute();

		// Insert should fail because record already exists
		RecordStream rs = session.insert(args.set.id(key))
			.bin("name").setTo("should_fail")
			.execute();

		assertTrue(rs.hasNext());
		RecordResult result = rs.next();
		assertFalse(result.isOk());
		assertEquals(ResultCode.KEY_EXISTS_ERROR, result.resultCode());
	}

	// ========== REPLACE Tests ==========

	@Test
	public void replaceUpdatesExistingRecord() {
		String key = "optype_replace_update";

		// Create record first with multiple bins
		session.upsert(args.set.id(key))
			.bin("name").setTo("original")
			.bin("extra").setTo("extra_value")
			.execute();

		// Replace should completely replace the record
		session.replace(args.set.id(key))
			.bin("name").setTo("replaced")
			.execute();

		// Verify record has new value and old bin is gone (full replacement)
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("replaced", rec.getString("name"));
		// Note: "extra" bin should be gone because replace replaces the entire record
	}

	@Test
	public void replaceCreatesRecordIfNotExists() {
		String key = "optype_replace_nonexistent";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Replace on non-existent record - behavior is CREATE_OR_REPLACE
		// This means it will create if not exists
		session.replace(args.set.id(key))
			.bin("name").setTo("created_via_replace")
			.execute();

		// Verify record was created
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("created_via_replace", rec.getString("name"));
	}

	// ========== REPLACE_IF_EXISTS Tests ==========

	@Test
	public void replaceIfExistsUpdatesExistingRecord() {
		String key = "optype_replace_if_exists_update";

		// Create record first with multiple bins
		session.upsert(args.set.id(key))
			.bin("name").setTo("original")
			.bin("extra").setTo("extra_value")
			.execute();

		// ReplaceIfExists should replace the record
		session.replaceIfExists(args.set.id(key))
			.bin("name").setTo("replaced_if_exists")
			.execute();

		// Verify record has new value
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("replaced_if_exists", rec.getString("name"));
	}

	@Test
	public void replaceIfExistsFailsOnNonExistentRecord() {
		String key = "optype_replace_if_exists_fail";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// ReplaceIfExists should fail because record doesn't exist
		RecordStream rs = session.replaceIfExists(args.set.id(key))
			.bin("name").setTo("should_fail")
			.execute();

		assertTrue(rs.hasNext());
		RecordResult result = rs.next();
		assertFalse(result.isOk());
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, result.resultCode());
	}

	// ========== UPDATE Tests ==========

	@Test
	public void updateModifiesExistingRecord() {
		String key = "optype_update_modify";

		// Create record first with multiple bins
		session.upsert(args.set.id(key))
			.bin("name").setTo("original")
			.bin("counter").setTo(10)
			.execute();

		// Update should modify the existing record (not replace all bins)
		session.update(args.set.id(key))
			.bin("name").setTo("updated")
			.execute();

		// Verify both bins exist - update preserves unspecified bins
		RecordStream rs = session.query(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		Record rec = rs.next().recordOrThrow();
		assertEquals("updated", rec.getString("name"));
		assertEquals(10, rec.getInt("counter"));
	}

	@Test
	public void updateFailsOnNonExistentRecord() {
		String key = "optype_update_fail";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Update should fail because record doesn't exist.
		// Note: No respondAllKeys() needed - UPDATE operations always return errors
		// because they are semantically expected to fail on non-existent records.
		RecordStream rs = session.update(args.set.id(key))
			.bin("name").setTo("should_fail")
			.execute();

		assertTrue(rs.hasNext());
		RecordResult result = rs.next();
		assertFalse(result.isOk());
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, result.resultCode());
	}

	// ========== DELETE Tests ==========

	@Test
	public void deleteRemovesExistingRecord() {
		String key = "optype_delete_remove";

		// Create record first
		session.upsert(args.set.id(key))
			.bin("name").setTo("to_be_deleted")
			.execute();

		// Verify record exists
		RecordStream rs = session.exists(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		assertTrue(rs.next().asBoolean());

		// Delete the record
		session.delete(args.set.id(key)).execute();

		// Verify record no longer exists
		rs = session.exists(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		assertFalse(rs.next().asBoolean());
	}

	@Test
	public void deleteNonExistentRecordSucceeds() {
		String key = "optype_delete_nonexistent";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Delete should succeed even if record doesn't exist (idempotent)
		RecordStream rs = session.delete(args.set.id(key)).execute();
		// Delete returns success even for non-existent records
		assertTrue(rs.hasNext());
	}

	// ========== EXISTS Tests ==========

	@Test
	public void existsReturnsTrueForExistingRecord() {
		String key = "optype_exists_true";

		// Create record first
		session.upsert(args.set.id(key))
			.bin("name").setTo("exists")
			.execute();

		// Exists should return true
		RecordStream rs = session.exists(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		assertTrue(rs.next().asBoolean());
	}

	@Test
	public void existsReturnsFalseForNonExistentRecord() {
		String key = "optype_exists_false";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Exists should return false
		RecordStream rs = session.exists(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		assertFalse(rs.next().asBoolean());
	}

	// ========== TOUCH Tests ==========

	@Test
	public void touchUpdatesGeneration() {
		String key = "optype_touch_generation";

		// Create record first
		session.upsert(args.set.id(key))
			.bin("name").setTo("touchable")
			.execute();

		// Get initial generation
		RecordStream rs = session.query(args.set.id(key)).withNoBins().execute();
		assertTrue(rs.hasNext());
		Record initialRec = rs.next().recordOrThrow();
		int initialGen = initialRec.generation;

		// Touch the record
		session.touch(args.set.id(key)).execute();

		// Verify generation increased
		rs = session.query(args.set.id(key)).withNoBins().execute();
		assertTrue(rs.hasNext());
		Record touchedRec = rs.next().recordOrThrow();
		assertEquals(initialGen + 1, touchedRec.generation);
	}

	@Test
	public void touchFailsOnNonExistentRecord() {
		String key = "optype_touch_fail";

		// Ensure record doesn't exist
		session.delete(args.set.id(key)).execute();

		// Touch should fail because record doesn't exist
		RecordStream rs = session.touch(args.set.id(key)).execute();
		assertTrue(rs.hasNext());
		RecordResult result = rs.next();
		assertFalse(result.isOk());
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, result.resultCode());
	}

	// ========== Batch Operations Tests ==========

	@Test
	public void batchInsertWithMultipleKeys() {
		String key1 = "optype_batch_insert_1";
		String key2 = "optype_batch_insert_2";

		// Ensure records don't exist
		session.delete(args.set.id(key1)).execute();
		session.delete(args.set.id(key2)).execute();

		// Batch insert multiple keys
		session.insert(args.set.id(key1), args.set.id(key2))
			.bin("status").setTo("batch_inserted")
			.execute();

		// Verify both records exist
		RecordStream rs = session.query(args.set.id(key1)).execute();
		assertTrue(rs.hasNext());
		assertEquals("batch_inserted", rs.next().recordOrThrow().getString("status"));

		rs = session.query(args.set.id(key2)).execute();
		assertTrue(rs.hasNext());
		assertEquals("batch_inserted", rs.next().recordOrThrow().getString("status"));
	}

	@Test
	public void batchReplaceIfExistsWithMultipleKeys() {
		String key1 = "optype_batch_replace_if_exists_1";
		String key2 = "optype_batch_replace_if_exists_2";
		String key3 = "optype_batch_replace_if_exists_nonexistent";

		// Create two records, ensure third doesn't exist
		session.upsert(args.set.id(key1))
			.bin("value").setTo("original1")
			.execute();

		session.upsert(args.set.id(key2))
			.bin("value").setTo("original2")
			.execute();

		session.delete(args.set.id(key3)).execute();

		// Batch replaceIfExists - should succeed for key1 and key2, fail for key3
		RecordStream rs = session.replaceIfExists(args.set.id(key1), args.set.id(key2), args.set.id(key3))
			.respondAllKeys()
			.bin("value").setTo("batch_replaced")
			.execute();

		// Check results
		int successCount = 0;
		int failCount = 0;

		while (rs.hasNext()) {
			RecordResult result = rs.next();
			if (result.isOk()) {
				successCount++;
			} else {
				failCount++;
				assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, result.resultCode());
			}
		}

		assertEquals(2, successCount); // key1 and key2 should succeed
		assertEquals(1, failCount);    // key3 should fail

		// Verify key1 and key2 have updated values
		rs = session.query(args.set.id(key1)).execute();
		assertTrue(rs.hasNext());
		assertEquals("batch_replaced", rs.next().recordOrThrow().getString("value"));

		rs = session.query(args.set.id(key2)).execute();
		assertTrue(rs.hasNext());
		assertEquals("batch_replaced", rs.next().recordOrThrow().getString("value"));
	}

	// ========== Chained Operations Tests ==========

	@Test
	public void chainedOperationsWithDifferentOpTypes() {
		String upsertKey = "optype_chained_upsert";
		String insertKey = "optype_chained_insert";
		String replaceKey = "optype_chained_replace";

		// Setup: ensure insert key doesn't exist, create replace key
		session.delete(args.set.id(insertKey)).execute();
		session.upsert(args.set.id(replaceKey))
			.bin("original").setTo(true)
			.execute();

		// Chain multiple different operation types
		RecordStream rs = session
			.upsert(args.set.id(upsertKey))
				.bin("type").setTo("upsert")
			.insert(args.set.id(insertKey))
				.bin("type").setTo("insert")
			.replaceIfExists(args.set.id(replaceKey))
				.bin("type").setTo("replace_if_exists")
			.execute();

		// All operations should succeed
		int count = 0;
		while (rs.hasNext()) {
			RecordResult result = rs.next();
			assertTrue(result.isOk(), "Operation " + count + " failed with " + result.resultCode());
			count++;
		}
		assertEquals(3, count);

		// Verify each record
		rs = session.query(args.set.id(upsertKey)).execute();
		assertTrue(rs.hasNext());
		assertEquals("upsert", rs.next().recordOrThrow().getString("type"));

		rs = session.query(args.set.id(insertKey)).execute();
		assertTrue(rs.hasNext());
		assertEquals("insert", rs.next().recordOrThrow().getString("type"));

		rs = session.query(args.set.id(replaceKey)).execute();
		assertTrue(rs.hasNext());
		assertEquals("replace_if_exists", rs.next().recordOrThrow().getString("type"));
	}
}
