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
package com.aerospike.client.fluent.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.info.classes.IndexType;
import com.aerospike.client.fluent.task.IndexTask;
import org.junit.jupiter.api.Test;
import com.aerospike.client.fluent.*;

public class QueryIndexTest extends ClusterTest {
	private static final String indexName = "testindex";
	private static final String binName = "testbin";

	@Test
	public void createDrop() {
		IndexTask task;

		try {
			task = session.dropIndex(args.set, indexName);
			task.waitTillComplete();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_NOTFOUND) {
				throw ae;
			}
		}

		task = session.createIndex(args.set, indexName, binName, IndexType.NUMERIC, IndexCollectionType.DEFAULT);
		task.waitTillComplete();

		task = session.dropIndex(args.set, indexName);
		task.waitTillComplete();

		Node[] nodes = cluster.getNodes();

		for (Node node : nodes) {
			String cmd = IndexTask.buildStatusCommand(args.namespace, indexName, node.getVersion());
			String response = Info.request(node, cmd);
			int code = Info.parseResultCode(response);

			assertEquals(201, code);
		}
	}

	@Test
	public void ctxRestore() {
		CTX[] ctx1 = new CTX[] {
			CTX.listIndex(-1),
			CTX.mapKey(Value.get("key1")),
			CTX.listValue(Value.get(937))
		};

		String base64 = CTX.toBase64(ctx1);
		CTX[] ctx2 = CTX.fromBase64(base64);

		assertEquals(ctx1.length, ctx2.length);

		for (int i = 0; i < ctx1.length; i++) {
			CTX item1 = ctx1[i];
			CTX item2 = ctx2[i];

			assertEquals(item1.id, item2.id);

			Object obj1 = item1.value.getObject();
			Object obj2 = item2.value.getObject();

			if (obj1 instanceof Integer && obj2 instanceof Long) {
				assertEquals((long)(Integer)obj1, (long)(Long)obj2);
			}
			else {
				assertEquals(obj1, obj2);
			}
		}
	}
}
