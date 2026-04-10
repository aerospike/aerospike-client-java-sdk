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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.tend.Partition;

/**
 * Paginate all record that belongs to a specific partition
 */
public class QueryWithPartitionPaginationTest extends ClusterTest {

	private static final int TARGET_PARTITION = 10;
	private static final int CHUNK_SIZE = 18;
	private static final int TOTAL_LIMIT = 90;
	private static final int NUM_RECORDS = 150;

	private static final String KEY_PREFIX = "pq_";
	private static final String BIN_NAME = "bin1";
	private long inserted = 0L;

	private final DataSet dataSet = DataSet.of(args.namespace, "test_pagination");

	@BeforeEach
	public void loadRecords() {
		session.truncate(dataSet);
		long candidate = 0L;
		while(inserted < NUM_RECORDS) {
			Key key = dataSet.id(KEY_PREFIX + candidate);
			candidate++;
			if (Partition.getPartitionId(key.digest) != TARGET_PARTITION) {
				continue;
			}
			session.upsert(key)
					.bin(BIN_NAME).setTo(inserted)
					.execute();
			inserted++;
		}
		assertEquals(NUM_RECORDS, inserted);
	}

	@AfterEach
	public void truncateAfter() {
		try {
			session.truncate(dataSet);
		}
		catch (RuntimeException ignored) {}
	}

	@Test
	public void shouldPaginateRecordForSpecificPartition() {
		long totalFromQuery = 0;
		try (RecordStream rs = session.query(dataSet)
				.onPartition(TARGET_PARTITION)
				.readingOnlyBins(BIN_NAME)
				.limit(TOTAL_LIMIT)
				.chunkSize(CHUNK_SIZE)
				.execute()) {
			while (rs.hasMoreChunks()) {
				while(rs.hasNext()) {
					rs.next().recordOrThrow();
					totalFromQuery++;
				}
			}
		}
		assertEquals(TOTAL_LIMIT, totalFromQuery);
	}
}
