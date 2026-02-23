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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.tend.Partition;
import com.aerospike.client.fluent.tend.Partitions;
import com.aerospike.util.ASNodeController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * <p>
 * Continue processing scans when "Partition unavailable" errors occur.
 * "Partition unavailable" is not a fatal error for partition scans and the server
 * will continue sending back results for other partitions. Previous clients aborted the scan and
 * put the connection back into the pool which might cause unprocessed results to be sent to a
 * different transaction.
 * </p>
 *
 * <p><b>Problem:</b> Client is hitting a cluster that has split brain issues and the cluster returns
 * unavailable partitions to clients as expected.</p>
 *
 * <p><b>Symptoms:</b></p>
 * <ul>
 *     <li>(a) Null bins returned when reading the record.</li>
 *     <li>(b) Writes failing with ClusterError.</li>
 *     <li>(c) Unable to get the Bin status while doing UDF operations.</li>
 *     <li>(d) Info command can fail with "Invalid prototype" error.</li>
 *     <li>(e) Data of some other key returned when reading.</li>
 * </ul>
 * <p><b>Note:</b> This test currently works with a cluster using the default Aerospike configuration file.</p>
 */
public class NodeChurnPartitionBehaviorTest extends ClusterTest {

	private static final Logger logger = Logger.getLogger(NodeChurnPartitionBehaviorTest.class.getName());

	private static final int SCAN_THREADS = 4;
	private static final int NUM_RECORDS = 1000;
	private static final long TASK_TIMEOUT = 5;

	private static final int PUT_ITERATIONS = 10;
	private static final int SCAN_ITERATIONS = PUT_ITERATIONS * 10;
	private static final int NODE_DOWN_ITERATIONS = PUT_ITERATIONS / 2;
	private static final String BIN_NAME = "bin1";
	private static final Random random = new Random();
	private Session upsertSession;
	private Session scanSession;
	private Session querySession;
	private ASNodeController asNodeController;


	@BeforeEach
	public void testInit() throws Exception {
		skipIfNotApplicable();
		asNodeController = new ASNodeController(args.containerNamePrefix, args.port);
		if (upsertSession ==  null) {
			Behavior upsertBehavior = Behavior.DEFAULT.deriveWithChanges("upsert-single-key", b -> b
					.on(Behavior.Selectors.reads().query(), ops -> ops
							.waitForCallToComplete(Duration.ofSeconds(10))
							.sendKey(true)
					)
			);
			upsertSession = cluster.createSession(upsertBehavior);
		}
		if (querySession == null) {
			Behavior queryBehavior = Behavior.DEFAULT.deriveWithChanges("get-single-key",
					b -> b.on(Behavior.Selectors.reads().get().ap(),
							ops -> ops.maximumNumberOfCallAttempts(5)
									.abandonCallAfter(Duration.ofSeconds(8))
									.delayBetweenRetries(Duration.ofMillis(500))
									.waitForCallToComplete(Duration.ofSeconds(10))
					)
			);
			querySession = cluster.createSession(queryBehavior);
		}
		if (scanSession == null) {
			Behavior scanBehavior = Behavior.DEFAULT.deriveWithChanges("scan-reads", b -> b
					.on(Behavior.Selectors.reads(), ops -> ops
							.waitForCallToComplete(Duration.ofSeconds(10))
							.maximumNumberOfCallAttempts(10)
							.delayBetweenRetries(Duration.ofMillis(1000))
					)
			);
			scanSession = cluster.createSession(scanBehavior);
		}
		// initial seeding
		writeNValidate(1, 1, true, false);
	}

	@Test
	public void shouldMaintainDataCorrectnessWhenPartitionUnavailableDuringNodeChurn() throws Exception {
		final int clusterSize = cluster.getNodes().length;
		long startTime = System.currentTimeMillis();
		logger.log(Level.INFO, "Starting partition churn test: clusterSize={0}, putIterations={1}, scanIterations={2}, nodeDownIterations={3}",
				new Object[]{clusterSize, PUT_ITERATIONS, SCAN_ITERATIONS, NODE_DOWN_ITERATIONS});

		// 1 thread for write_n_validate, 1 for node_down
		int maxWorkers = SCAN_THREADS + 1 + 1;
		ExecutorService executor = Executors.newFixedThreadPool(maxWorkers);
		try {
			List<CompletableFuture<?>> futures = new ArrayList<>();
			futures.add(CompletableFuture.runAsync(
					() -> writeNValidate(PUT_ITERATIONS, 10, false, true), executor)
					.orTimeout(TASK_TIMEOUT, TimeUnit.MINUTES)
			);
			futures.add(CompletableFuture.runAsync(() -> nodeDown(clusterSize), executor).orTimeout(TASK_TIMEOUT, TimeUnit.MINUTES));
			for (int t = 0; t < SCAN_THREADS; t++) {
				final int threadId = t;
				futures.add(CompletableFuture.runAsync(() -> scanRecs(threadId), executor).orTimeout(TASK_TIMEOUT, TimeUnit.MINUTES));
			}
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			long elapsed = System.currentTimeMillis() - startTime;
			logger.log(Level.INFO, "Partition churn test completed in {0} ms", elapsed);
		}
		finally {
			logger.log(Level.INFO, "Restoring all nodes and shutting down executor");
			asNodeController.startAllNodes();
			if (!executor.isTerminated()) {
				executor.shutdownNow();
				executor.awaitTermination(60, TimeUnit.SECONDS);
			}
		}

	}

	private void skipIfNotApplicable() {
		assumeTrue(args.namespace != null && !args.namespace.isEmpty(), "Namespace must be set (e.g. -n <namespace>)");
		assumeTrue(cluster.getNodes().length > 1, "Cluster should have at least two node");
		assumeTrue(!args.scMode, "Test not valid for strong-consistency scenarios");
	}

	private void writeNValidate(int iterations, int numUpdates,
								boolean throwExp, boolean validate) {
		logger.log(Level.FINE, "writeNValidate started: numRecords={0}, nIteration={1}, nUpdates={2}, validate={3}",
				new Object[]{NUM_RECORDS, iterations, numUpdates, validate});
		for (int itr = 0; itr < iterations; itr++) {
			if (itr > 0 && itr % 2 == 0) {
				logger.log(Level.FINE, "writeNValidate iteration {0}/{1}", new Object[]{itr, iterations});
			}
			for (int pk = 0; pk < NUM_RECORDS; pk++) {
				for (int n = 1; n <= numUpdates; n++) {
					String binVal = ("a" + pk + "a").repeat(n);
					try {
						upsertSession.upsert(args.set.id(pk))
								.bin(BIN_NAME).setTo(binVal)
								.withTotalTimeout(3000)
								.sendKey()
								.execute();

						if (validate) {
							RecordStream rs = querySession
									.query(args.set.id(pk))
									.execute();
							Record rec = rs.hasNext() ? rs.next().recordOrThrow() : null;
							assertNotNull(rec, "Record should exist for key " + pk);
							String got = rec.getString(BIN_NAME);
							if (got == null) {
								logPartitionInfo(args.namespace, args.set.getSet(), pk);
							}
							assertNotNull(got, "Null bins returned for key " + pk);
							if (!got.contains("a" + pk + "a")) {
								logPartitionInfo(args.namespace, args.set.getSet(), pk);
							}
							assertTrue(got.contains("a" + pk + "a"),
									"Expected value to contain a" + pk + "a but got: " + got);
						}
					} catch (AerospikeException e) {
						// PartitionUnavailable may occur during node churn when looking up a key.
                        // Ignore it and retry with upsert + query.
						if (e.getResultCode() == ResultCode.PARTITION_UNAVAILABLE
								|| (!throwExp && (e.getResultCode() == ResultCode.TIMEOUT
								|| e instanceof  AerospikeException.Connection
								|| e.getResultCode() == ResultCode.SERVER_NOT_AVAILABLE))) {
							logger.log(Level.WARNING, "Retrying after exception for pk={0}: {1}", new Object[]{pk, e.getMessage()});
							sleep(500);
							continue;
						}
						throw e;
					}
				}
			}
		}
	}

	private void scanRecs(int threadId) {
		int retryCount = 0;
		for (int i = 0; i < SCAN_ITERATIONS; i++) {
			try (RecordStream rs = scanSession.query(args.set)
					.readingOnlyBins(BIN_NAME)
					.execute()) {
				while (rs.hasNext()) {
					RecordResult rr = rs.next();
					rr.recordOrThrow();
				}
				if (i > 0 && i % 20 == 0) {
					logger.log(Level.FINE, "Scan thread {0}: completed {1}/{2} iterations", new Object[]{threadId, i, SCAN_ITERATIONS});
				}
			} catch (AerospikeException e) {
				if (e.getResultCode() == ResultCode.TIMEOUT
						|| e instanceof AerospikeException.Connection
						|| e.getResultCode() == ResultCode.SERVER_NOT_AVAILABLE) {
					retryCount++;
					if (retryCount <= 5 || retryCount % 50 == 0) {
						logger.log(Level.WARNING, "Scan thread {0}: retry {1} after {2} (iteration {3})",
								new Object[]{threadId, retryCount, e.getResultCode(), i});
					}
					sleep(1000);
					continue;
				}
				throw e;
			}
		}
		logger.log(Level.FINE, "Scan thread {0} finished: {1} iterations, {2} retries", new Object[]{threadId, SCAN_ITERATIONS, retryCount});
	}

	private void nodeDown(int clusterSize) {
		try {
			for (int i = 0; i < NODE_DOWN_ITERATIONS; i++) {
				int node = random.nextInt(clusterSize) + 1;
				asNodeController.stopNode(node);
				int delay = 500 * (random.nextInt(3) + 1);
				sleep(delay);
				asNodeController.startNode(node, null);
			}
			logger.log(Level.FINE, "Node churn finished");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void logPartitionInfo(String namespace, String set, int pk) {
		Key key = new Key(namespace, set, pk);
		byte[] digest = key.digest;
		int pid = Partition.getPartitionId(digest);

		Partitions partitions = cluster.getPartitions(namespace);
		for (int index = 0; index < partitions.replicas.length; index++) {
			Node node = partitions.replicas[index].get(pid);
			if (node != null) {
				logger.log(Level.WARNING,
						"partition info for PID {0} on node {1} (replica {2})",
						new Object[]{pid, node, index}
				);
			}
		}
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException ie) {
			// ignore
		}
	}

	@AfterEach
    public void cleanUp() {
		querySession = null;
		scanSession = null;
		upsertSession = null;
	}

}
