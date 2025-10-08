/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.fluent.policy;

/**
 * Command policy attributes used in all database commands.
 */
public class Policy {
	/**
	 * Replica algorithm used to determine the target node for a partition derived from a key
	 * or requested in a scan/query.
	 * <p>
	 * Default: {@link Replica#SEQUENCE}
	 */
	public final Replica replica;

	/**
	 * Socket connect timeout in milliseconds. If connectTimeout greater than zero, it will
	 * be applied to creating a connection plus optional user authentication. Otherwise,
	 * socketTimeout or totalTimeout will be used depending on their values.
	 * <p>
	 * If connect, socket and total timeouts are zero, the actual socket connect timeout
	 * is hard-coded to 2000ms.
	 * <p>
	 * connectTimeout is useful when new connection creation is expensive (ie TLS connections)
	 * and it's acceptable to allow extra time to create a new connection compared to using an
	 * existing connection from the pool.
	 * <p>
	 * Default: 0
	 */
	public final int connectTimeout;

	/**
	 * Socket idle timeout in milliseconds when processing a database command.
	 * <p>
	 * If socketTimeout is zero and totalTimeout is non-zero, then socketTimeout will be set
	 * to totalTimeout.  If both socketTimeout and totalTimeout are non-zero and
	 * socketTimeout &gt; totalTimeout, then socketTimeout will be set to totalTimeout. If both
	 * socketTimeout and totalTimeout are zero, then there will be no socket idle limit.
	 * <p>
	 * If socketTimeout is non-zero and the socket has been idle for at least socketTimeout,
	 * both maxRetries and totalTimeout are checked.  If maxRetries and totalTimeout are not
	 * exceeded, the command is retried.
	 * <p>
	 * For synchronous methods, socketTimeout is the socket timeout (SO_TIMEOUT).
	 * For asynchronous methods, the socketTimeout is implemented using a HashedWheelTimer.
	 * <p>
	 * Default: 30000ms
	 */
	public final int socketTimeout;

	/**
	 * Total command timeout in milliseconds.
	 * <p>
	 * The totalTimeout is tracked on the client and sent to the server along with
	 * the command in the wire protocol.  The client will most likely timeout
	 * first, but the server also has the capability to timeout the command.
	 * <p>
	 * If totalTimeout is not zero and totalTimeout is reached before the command
	 * completes, the command will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * If totalTimeout is zero, there will be no total time limit on the client side.
	 * However, the server converts zero timeouts to the server configuration field
	 * transaction-max-ms (default 1000ms) for all commands except queries. For short
	 * queries {@link QueryDuration#SHORT}, the server converts zero timeouts to a
	 * hard-coded 1000ms. For long queries, there is no timeout conversion on the
	 * server.
	 * <p>
	 * Default for scan/query: 0
	 * <p>
	 * Default for all other commands: 1000ms
	 */
	public final int totalTimeout;

	/**
	 * Delay milliseconds after socket read timeout in an attempt to recover the socket
	 * in the background.  Processing continues on the original command and the user
	 * is still notified at the original command timeout.
	 * <p>
	 * When a command is stopped prematurely, the socket must be drained of all incoming
	 * data or closed to prevent unread socket data from corrupting the next command
	 * that would use that socket.
	 * <p>
	 * If a socket read timeout occurs and timeoutDelay is greater than zero, the socket
	 * will be drained until all data has been read or timeoutDelay is reached.  If all
	 * data has been read, the socket will be placed back into the connection pool.  If
	 * timeoutDelay is reached before the draining is complete, the socket will be closed.
	 * <p>
	 * Sync sockets are drained in the cluster tend thread at periodic intervals.  Async
	 * sockets are drained in the event loop from which the async command executed.
	 * <p>
	 * Many cloud providers encounter performance problems when sockets are closed by the
	 * client when the server still has data left to write (results in socket RST packet).
	 * If the socket is fully drained before closing, the socket RST performance penalty
	 * can be avoided on these cloud providers.
	 * <p>
	 * The disadvantage of enabling timeoutDelay is that extra memory/processing is required
	 * to drain sockets and additional connections may still be needed for command retries.
	 * <p>
	 * If timeoutDelay were to be enabled, 3000ms would be a reasonable value.
	 * <p>
	 * Default: 0 (no delay, connection closed on timeout)
	 */
	public final int timeoutDelay;

	/**
	 * Maximum number of retries before aborting the current command.
	 * The initial attempt is not counted as a retry.
	 * <p>
	 * If maxRetries is exceeded, the command will abort with
	 * {@link com.aerospike.client.AerospikeException.Timeout}.
	 * <p>
	 * WARNING: Database writes that are not idempotent (such as add())
	 * should not be retried because the write operation may be performed
	 * multiple times if the client timed out previous command attempts.
	 * It's important to use a distinct WritePolicy for non-idempotent
	 * writes which sets maxRetries = 0;
	 * <p>
	 * Default for write: 0 (no retries)
	 * <p>
	 * Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	 * <p>
	 * Default for scan/query: 5 (6 attempts. See {@link ScanPolicy#ScanPolicy()} comments.)
	 */
	public final int maxRetries;

	/**
	 * Milliseconds to sleep between retries.  Enter zero to skip sleep.
	 * This field is ignored when maxRetries is zero.
	 * This field is also ignored in async mode.
	 * <p>
	 * The sleep only occurs on connection errors and server timeouts
	 * which suggest a node is down and the cluster is reforming.
	 * The sleep does not occur when the client's socketTimeout expires.
	 * <p>
	 * Reads do not have to sleep when a node goes down because the cluster
	 * does not shut out reads during cluster reformation.  The default for
	 * reads is zero.
	 * <p>
	 * The default for writes is also zero because writes are not retried by default.
	 * Writes need to wait for the cluster to reform when a node goes down.
	 * Immediate write retries on node failure have been shown to consistently
	 * result in errors.  If maxRetries is greater than zero on a write, then
	 * sleepBetweenRetries should be set high enough to allow the cluster to
	 * reform (&gt;= 3000ms).
	 * <p>
	 * Default: 0 (do not sleep between retries)
	 */
	public final int sleepBetweenRetries;

	/**
	 * Determine how record TTL (time to live) is affected on reads. When enabled, the server can
	 * efficiently operate as a read-based LRU cache where the least recently used records are expired.
	 * The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	 * within this interval of the record’s end of life will generate a touch.
	 * <p>
	 * For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	 * 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	 * recent write) will result in a touch, resetting the TTL to another 10 hours.
	 * <p>
	 * Values:
	 * <ul>
	 * <li> 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.</li>
	 * <li>-1 : Do not reset record TTL on reads.</li>
	 * <li>1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.</li>
	 * </ul>
	 * <li>
	 * <p>
	 * Default: 0
	 */
	public final int readTouchTtlPercent;

	/**
	 * Send user defined key in addition to hash digest on both reads and writes.
	 * If the key is sent on a write, the key will be stored with the record on
	 * the server.
	 * <p>
	 * If the key is sent on a read, the server will generate the hash digest from
	 * the key and validate that digest with the digest sent by the client. Unless
	 * this is the explicit intent of the developer, avoid sending the key on reads.
	 * <p>
	 * Default: false (do not send the user defined key)
	 */
	public final boolean sendKey;

	/**
	 * Use zlib compression on command buffers sent to the server and responses received
	 * from the server when the buffer size is greater than 128 bytes. This option will
	 * increase cpu and memory usage (for extra compressed buffers), but decrease the size
	 * of data sent over the network.
	 * <p>
	 * This compression feature requires the Enterprise Edition Server.
	 * <p>
	 * Default: false
	 */
	public final boolean compress;

	/**
	 * Copy policy from dynamic configuration policy.
	 */
	public Policy(SettablePolicy other) {
        // This has no equivalent in the underlying code
		this.replica = (other.replicaOrder != null)?
			Replica.SEQUENCE : Replica.SEQUENCE;

		this.connectTimeout = (other.waitForConnectionToComplete != null)?
			other.waitForConnectionToComplete : 0;

		this.socketTimeout =  (other.waitForCallToComplete != null)?
			other.waitForCallToComplete : 30000;

		this.totalTimeout = (other.abandonCallAfter != null)?
			other.abandonCallAfter : 1000;

    	this.timeoutDelay = (other.waitForSocketResponseAfterCallFails != null)?
    		other.waitForSocketResponseAfterCallFails : 0;

		this.maxRetries = (other.maximumNumberOfCallAttempts != null)?
        	other.maximumNumberOfCallAttempts - 1 : 2;

    	this.sleepBetweenRetries = (other.delayBetweenRetries != null)?
    		other.delayBetweenRetries : 0;

    	this.readTouchTtlPercent = (other.resetTtlOnReadAtPercent != null)?
    		other.resetTtlOnReadAtPercent : 0;

    	this.sendKey = (other.sendKey != null)?
    		other.sendKey : false;

    	this.compress = (other.useCompression != null)?
    		other.useCompression : false;
	}
}
