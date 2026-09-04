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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.DataSet;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.policy.QueryDuration;
import com.aerospike.client.sdk.util.Version;

public class QueryRPSTest extends ClusterTest {
    private static final DataSet dataSet = DataSet.of(args.set.getNamespace(), "rps");
    private static final String indexName = "rpsindex";
    private static final String keyPrefix = "rpskey";
    private static final String binName1 = "rpsbin1";
    private static final String binName2 = "rpsbin2";
    //private static final String binName3 = "rpsbin3";
    private static final int records_per_node = 1000;
    private static final int rps = 1000;
    private static final int expected_duration = 1000 * records_per_node / rps;

    private static int n_records = 0;

    @BeforeAll
    public static void prepare() {
        try {
            session.createIndex(dataSet, indexName, binName1, IndexType.INTEGER,
                IndexCollectionType.DEFAULT).waitTillComplete();
        }
        catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }

        n_records = records_per_node * cluster.getNodes().length;

        session.truncate(dataSet);

        for (int i = 1; i <= n_records; i++) {
            session.upsert(dataSet.ids(keyPrefix + i)).bins(binName1, binName2).values(i, i)
                .execute();
        }
    }

    @AfterAll
    public static void destroy() {
        session.dropIndex(dataSet, indexName);
    }

    @SuppressWarnings("unused")
    private void checkRuntime(Node n, long id) {
        String taskId = Long.toUnsignedString(id);
        Version serverVersion = n.getVersion();
        String command = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1)
            ? "query-show:id=" + taskId : "query-show:trid=" + taskId;

        String job_info = Info.request(n, command);
        String s = "run-time=";
        int runStart = job_info.indexOf(s) + s.length();

        job_info = job_info.substring(runStart, job_info.indexOf(':', runStart));

        int duration = Integer.parseInt(job_info);

        assert (duration > expected_duration - 500 && duration < expected_duration + 500);
    }

    void drainRecords(RecordStream rs) {
        try {
            while (rs.hasNext()) {
            }
        }
        finally {
            rs.close();
        }
    }

    /**
     * A records-per-second limit is invalid on a short query, and the rejection must reach the
     * caller. The server refuses the combination while setting the job up:
     *
     * <pre>
     * // as/src/query/query.c - get_query_rps()
     * if (as_transaction_is_short_query(tr)) {
     *     cf_warning(AS_QUERY, "short query has recs-per-sec field");
     *     return false;                      // basic_query_job_start returns AS_ERR_PARAMETER
     * }
     * </pre>
     *
     * <p>The client sends both the {@code RECORDS_PER_SECOND} field and {@code INFO1_SHORT_QUERY}
     * without objecting, so the refusal arrives after {@code execute()} has already handed back a
     * stream. It therefore reaches the caller through the stream rather than being thrown by
     * {@code execute()}, which is what makes this worth an integration test: before CLIENT-5406 the
     * executor completed the stream normally on failure, so a rejected query was indistinguishable
     * from one that matched nothing.</p>
     *
     * <p>Runs on any namespace. {@code QuerySelectionHintExecuteTest} covers the same contract via
     * {@code LONG_RELAX_AP}, but only on strong consistency, so it is skipped on an AP cluster.</p>
     */
    @Test
    public void shortQueryWithRecordsPerSecondReportsRejection() {
        AerospikeException ae = assertThrows(AerospikeException.class, () -> {
            try (RecordStream rs = session.query(dataSet)
                .recordsPerSecond(rps)
                .withHint(hint -> hint.queryDuration(QueryDuration.SHORT))
                .execute()) {

                while (rs.hasNext()) {
                    rs.next();
                }
            }
        });

        assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
    }

    @Test
    public void scan() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        long taskId = new RandomShift().nextLong();

        RecordStream rs = session.query(dataSet)
        	.recordsPerSecond(rps)
        	.execute();

        drainRecords(rs);

        for (Node n : cluster.getNodes()) {
        	checkRuntime(n, stmt, taskId);
        }
        */
    }

    @Test
    public void bgScanWithOps() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        Statement stmt = new Statement();
        stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
        stmt.setRecordsPerSecond(rps);

        ExecuteTask task = client.execute(null, stmt, Operation.put(
        		new Bin(binName3, 1)));

        task.waitTillComplete();

        for (Node n : client.getNodes()) {
        	checkRuntime(n, stmt, task.getTaskId());
        }
        */
    }

    @Test
    public void bgScanWithUDF() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        Statement stmt = new Statement();
        stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
        stmt.setRecordsPerSecond(rps);

        ExecuteTask task = client.execute(null, stmt, "record_example",
        		"processRecord", Value.get(binName2), Value.get(binName2),
        		Value.get(100));

        task.waitTillComplete();

        for (Node n : client.getNodes()) {
        	checkRuntime(n, stmt, task.getTaskId());
        }
        */
    }

    @Test
    public void query() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        long taskId = new RandomShift().nextLong();

        Statement stmt = new Statement();
        stmt.setNamespace(args.namespace);
        stmt.setSetName(args.set);
        stmt.setFilter(Filter.range(binName1, 0, n_records));
        stmt.setRecordsPerSecond(rps);
        stmt.setTaskId(taskId);

        RecordSet rs = client.query(null, stmt);

        drainRecords(rs);

        for (Node n : client.getNodes()) {
        	checkRuntime(n, stmt, taskId);
        }
        */
    }

    @Test
    public void bgQueryWithOps() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        Statement stmt = new Statement();
        stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
        stmt.setFilter(Filter.range(binName1, 0, n_records));
        stmt.setRecordsPerSecond(rps);

        ExecuteTask task = client.execute(null, stmt, Operation.put(
        		new Bin(binName3, 1)));

        task.waitTillComplete();

        for (Node n : client.getNodes()) {
        	checkRuntime(n, stmt, task.getTaskId());
        }
        */
    }

    @Test
    public void bgQueryWithUDF() {
        /* TODO Implement when setting or retrieving the taskId is supported.
        Statement stmt = new Statement();
        stmt.setNamespace(args.namespace); stmt.setSetName(args.set);
        stmt.setFilter(Filter.range(binName1, 0, n_records));
        stmt.setRecordsPerSecond(rps);

        ExecuteTask task = client.execute(null, stmt, "record_example",
        		"processRecord", Value.get(binName2), Value.get(binName2),
        		Value.get(100));

        task.waitTillComplete();

        for (Node n : client.getNodes()) {
        	checkRuntime(n, stmt, task.getTaskId());
        }
        */
    }
}