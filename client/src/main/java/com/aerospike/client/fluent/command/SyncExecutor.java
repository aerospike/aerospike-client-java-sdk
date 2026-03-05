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
package com.aerospike.client.fluent.command;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.metrics.LatencyType;
import com.aerospike.client.fluent.tend.ConnectionRecover;
import com.aerospike.client.fluent.util.Util;

public abstract class SyncExecutor {
	Cluster cluster;
	Command cmd;
	ArrayList<AerospikeException> subExceptions;
	int socketTimeout;
	int totalTimeout;
	int iteration = 1;
	int commandSentCounter;
	long deadline;

	public SyncExecutor(Cluster cluster, Command cmd) {
		this.cluster = cluster;
		this.cmd = cmd;
		this.socketTimeout = cmd.socketTimeout;
		this.totalTimeout = cmd.totalTimeout;
	}

	/**
	 * Scan/Query constructor.
	 */
	/*
	public SyncCommand(Cluster cluster, Policy policy, int socketTimeout, int totalTimeout, String namespace) {
		super(socketTimeout, totalTimeout, 0);
		this.cluster = cluster;
		this.policy = policy;
		this.namespace = namespace;
	}
	*/

	public final void execute() {
		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);
		}
		executeCommand();
	}

	public final void executeCommand() {
		Node node;
		AerospikeException exception = null;
		//long begin = 0;
		//boolean metricsEnabled = cluster.metricsEnabled;
		//LatencyType latencyType = metricsEnabled? getLatencyType() : LatencyType.NONE;
		boolean isClientTimeout;

		// Execute command until successful, timed out or maximum iterations have been reached.
		while (true) {
			try {
				node = getNode();
			}
			catch (AerospikeException ae) {
				if (cluster.isActive()) {
					prepareException(null, ae, subExceptions);
					throw ae;
				}
				else {
					throw new AerospikeException("Cluster has been closed");
				}
			}

			try {
				node.validateErrorCount();

				/*
				if (latencyType != LatencyType.NONE) {
					begin = System.nanoTime();
				}
				*/

				Connection conn = node.getConnection(this, cmd.connectTimeout, socketTimeout, cmd.timeoutDelay);

				try {
					// Set command buffer.
					CommandBuffer cb = getCommandBuffer();

					// Send command.
					conn.write(cb.getBuffer(), cb.getLength());
					commandSentCounter++;

					/*
					if (metricsEnabled) {
						node.addBytesOut(namespace, dataOffset);
					}
					*/

					// Parse results.
					parseResult(node, conn, cb.getBuffer());

					// Put connection back in pool.
					node.putConnection(conn);

					/*
					if (latencyType != LatencyType.NONE) {
						long elapsed = System.nanoTime() - begin;
						node.addLatency(namespace, latencyType, elapsed);
					}
					*/

					// Command has completed successfully.  Exit method.
					return;
				}
				catch (AerospikeException ae) {
					if (ae.keepConnection()) {
						// Put connection back in pool.
						node.putConnection(conn);
					}
					else {
						// Close socket to flush out possible garbage.  Do not put back in pool.
						node.closeConnection(conn);
					}

					if (ae.getResultCode() == ResultCode.TIMEOUT) {
						// Retry on server timeout.
						// Log.info("Server timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
						exception = new AerospikeException.Timeout(cmd, false);
						isClientTimeout = false;
						node.incrErrorRate();
						node.addTimeout(cmd.namespace);
					}
					else if (ae.getResultCode() == ResultCode.DEVICE_OVERLOAD) {
						// Add to circuit breaker error count and retry.
						exception = ae;
						isClientTimeout = false;
						node.incrErrorRate();
						node.addError(cmd.namespace);
					}
					else if (ae.getResultCode() == ResultCode.KEY_BUSY) {
						exception = ae;
						isClientTimeout = false;
						node.incrErrorRate();
						node.addKeyBusy(cmd.namespace);
					}
					else {
						node.addError(cmd.namespace);
						prepareException(node, ae, subExceptions);
						throw ae;
					}
				}
				catch (Connection.ReadTimeout crt) {
					if (cmd.timeoutDelay > 0) {
						cluster.recoverConnection(new ConnectionRecover(conn, node, cmd.timeoutDelay, crt, isSingle()));
					}
					else {
						node.closeConnection(conn);
					}
					exception = new AerospikeException.Timeout(cmd, true);
					isClientTimeout = true;
					node.addTimeout(cmd.namespace);
				}
				catch (SocketTimeoutException ste) {
					// Full timeout has been reached.
					// Log.info("Socket timeout: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException.Timeout(cmd, true);
					isClientTimeout = true;
					node.addTimeout(cmd.namespace);
				}
				catch (IOException ioe) {
					// IO errors are considered temporary anomalies.  Retry.
					// Log.info("IOException: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					exception = new AerospikeException.Connection(ioe);
					isClientTimeout = false;
					node.addError(cmd.namespace);
				}
				catch (Throwable t) {
					// All remaining exceptions are considered fatal.  Do not retry.
					// Close socket to flush out possible garbage.  Do not put back in pool.
					// Log.info("Throw Throwable: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
					node.closeConnection(conn);
					node.addError(cmd.namespace);
					AerospikeException ae = new AerospikeException(t);
					prepareException(node, ae, subExceptions);
					throw ae;
				}
			}
			catch (Connection.ReadTimeout crt) {
				// Connection already handled.
				exception = new AerospikeException.Timeout(cmd, true);
				isClientTimeout = true;
				node.addTimeout(cmd.namespace);
			}
			catch (AerospikeException.Connection ce) {
				// Socket connection error has occurred. Retry.
				// Log.info("Connection error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = ce;
				isClientTimeout = false;
				node.addError(cmd.namespace);
			}
			catch (AerospikeException.Backoff be) {
				// Node is in backoff state. Retry, hopefully on another node.
				// Log.info("Backoff error: " + tranId + ',' + node + ',' + sequence + ',' + iteration);
				exception = be;
				isClientTimeout = false;
				node.addError(cmd.namespace);
			}
			catch (AerospikeException ae) {
				// Log.info("Throw AerospikeException: " + tranId + ',' + node + ',' + sequence + ',' + iteration + ',' + ae.getResultCode());
				node.addError(cmd.namespace);
				prepareException(node, ae, subExceptions);
				throw ae;
			}
			catch (Throwable t) {
				node.addError(cmd.namespace);
				AerospikeException ae = new AerospikeException(t);
				prepareException(node, ae, subExceptions);
				throw ae;
			}

			// Check maxRetries.
			if (iteration > cmd.maxRetries) {
				break;
			}

			if (totalTimeout > 0) {
				// Check for total timeout.
				long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(cmd.sleepBetweenRetries);

				if (remaining <= 0) {
					break;
				}

				// Convert back to milliseconds for remaining check.
				remaining = TimeUnit.NANOSECONDS.toMillis(remaining);

				if (remaining < totalTimeout) {
					totalTimeout = (int)remaining;

					if (socketTimeout > totalTimeout) {
						socketTimeout = totalTimeout;
					}
				}
			}

			if (!isClientTimeout && cmd.sleepBetweenRetries > 0) {
				// Sleep before trying again.
				Util.sleep(cmd.sleepBetweenRetries);
			}

			exception.setNode(node);
			exception.setCommand(cmd);
			exception.setIteration(iteration);
			exception.setInDoubt(isWrite(), commandSentCounter);
			addSubException(exception);
			iteration++;

			if (! prepareRetry(isClientTimeout || exception.getResultCode() != ResultCode.SERVER_NOT_AVAILABLE)) {
				// Batch may be retried in separate commands.
				if (retryBatch(cluster, socketTimeout, totalTimeout, deadline, iteration, commandSentCounter)) {
					// Batch was retried in separate commands.  Complete this command.
					return;
				}
			}

			cluster.addRetry();
		}

		// Retries have been exhausted.  Throw last exception.
		// Log.info("Runtime exception: " + tranId + ',' + sequence + ',' + iteration + ',' + exception.getMessage());
		prepareException(node, exception, subExceptions);
		throw exception;
	}

	protected void addSubException(AerospikeException exception) {
		if (subExceptions == null) {
			subExceptions = new ArrayList<AerospikeException>(cmd.maxRetries);
		}
		subExceptions.add(exception);
	}

	private void prepareException(Node node, AerospikeException ae, List<AerospikeException> subExceptions) {
		ae.setNode(node);
		ae.setCommand(cmd);
		ae.setIteration(iteration);
		ae.setInDoubt(isWrite(), commandSentCounter);
		ae.setSubExceptions(subExceptions);

		if (ae.getInDoubt()) {
			onInDoubt();
		}
	}

	protected void onInDoubt() {
		// Write commands will override this method.
	}

	public void resetDeadline(long startTime) {
		long elapsed = System.nanoTime() - startTime;
		deadline += elapsed;
	}

	/*
	@Override
	protected void sizeBuffer() {
		dataBuffer = new byte[dataOffset];
	}*/

	protected boolean retryBatch(
		Cluster cluster,
		int socketTimeout,
		int totalTimeout,
		long deadline,
		int iteration,
		int commandSentCounter
	) {
		return false;
	}

	protected boolean isSingle() {
		return true;
	}

	protected boolean isWrite() {
		return false;
	}

	protected abstract Node getNode();
	protected abstract LatencyType getLatencyType();
	protected abstract CommandBuffer getCommandBuffer();
	protected abstract void parseResult(Node node, Connection conn, byte[] buffer) throws IOException;
	protected abstract boolean prepareRetry(boolean timeout);
}
