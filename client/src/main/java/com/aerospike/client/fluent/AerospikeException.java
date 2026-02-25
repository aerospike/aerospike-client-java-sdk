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

import java.util.List;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.Command;
import com.aerospike.client.fluent.command.CommitError;
import com.aerospike.client.fluent.tend.Partition;

public class AerospikeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

    protected transient Node node;
    protected List<AerospikeException> subExceptions;
    protected int resultCode = ResultCode.CLIENT_ERROR;
	private int connectTimeout;
	private int socketTimeout;
	private int totalTimeout;
	private int maxRetries;
    protected int iteration = -1;
    protected boolean inDoubt;

    public AerospikeException(int resultCode, String message) {
        super(message);
        this.resultCode = resultCode;
    }

	public AerospikeException(int resultCode, Throwable e) {
		super(e);
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode) {
		super();
		this.resultCode = resultCode;
	}

	public AerospikeException(int resultCode, boolean inDoubt) {
		super();
		this.resultCode = resultCode;
		this.inDoubt = inDoubt;
	}

    public AerospikeException(int resultCode, String message, Throwable e) {
        super(message, e);
        this.resultCode = resultCode;
    }

    public AerospikeException(int resultCode, String message, boolean inDoubt) {
        super(message);
        this.resultCode = resultCode;
        this.inDoubt = inDoubt;
    }

    public AerospikeException(String message, Throwable e) {
        super(message, e);
    }

	public AerospikeException(String message) {
        super(message);
    }

	public AerospikeException(Throwable e) {
		super(e);
	}

	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder(512);

		sb.append("Error ");
		sb.append(resultCode);

		if (iteration >= 0) {
			sb.append(',');
			sb.append(iteration);
		}

		sb.append(',');
		sb.append(connectTimeout);
		sb.append(',');
		sb.append(socketTimeout);
		sb.append(',');
		sb.append(totalTimeout);
		sb.append(',');
		sb.append(maxRetries);

		if (inDoubt) {
			sb.append(",inDoubt");
		}

		if (node != null) {
			sb.append(',');
			sb.append(node.toString());
		}

		sb.append(": ");
		sb.append(getBaseMessage());

		if (subExceptions != null) {
			sb.append(System.lineSeparator());
			sb.append("sub-exceptions:");

			for (AerospikeException ae : subExceptions) {
				sb.append(System.lineSeparator());
				sb.append(ae.getMessage());
			}
		}
		return sb.toString();
	}

	/**
	 * Return base message without extra metadata.
	 */
	public String getBaseMessage() {
		String message = super.getMessage();
		return (message != null)? message : ResultCode.getResultString(resultCode);
	}

	/**
	 * Should connection be put back into pool.
	 */
	public final boolean keepConnection() {
		return ResultCode.keepConnection(resultCode);
	}

	/**
	 * Get last node used.
	 */
	public final Node getNode() {
		return node;
	}

	/**
	 * Set last node used.
	 */
	public final void setNode(Node node) {
		this.node = node;
	}

	/**
	 * Set command associated with the exception.
	 */
	public final void setCommand(Command cmd) {
		connectTimeout = cmd.getConnectTimeout();
		socketTimeout = cmd.getSocketTimeout();
		totalTimeout = cmd.getTotalTimeout();
		maxRetries = cmd.getMaxRetries();
	}

    public int getConnectTimeout() {
		return connectTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public int getTotalTimeout() {
		return totalTimeout;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	/**
	 * Get sub exceptions.  Will be null if a retry did not occur.
	 */
	public final List<AerospikeException> getSubExceptions() {
		return subExceptions;
	}

	/**
	 * Set sub exceptions.
	 */
	public final void setSubExceptions(List<AerospikeException> subExceptions) {
		this.subExceptions = subExceptions;
	}

	/**
	 * Get integer result code.
	 */
	public final int getResultCode() {
		return resultCode;
	}

	/**
	 * Get number of attempts before failing.
	 */
	public final int getIteration() {
		return iteration;
	}

	/**
	 * Set number of attempts before failing.
	 */
	public final void setIteration(int iteration) {
		this.iteration = iteration;
	}

	/**
	 * Is it possible that write command may have completed.
	 */
	public final boolean getInDoubt() {
		return inDoubt;
	}

	/**
	 * Sets the inDoubt value to inDoubt.
	 */
	public void setInDoubt(boolean inDoubt) {
		this.inDoubt = inDoubt;
	}

	/**
	 * Set whether it is possible that the write command may have completed
	 * even though this exception was generated.  This may be the case when a
	 * client error occurs (like timeout) after the command was sent to the server.
	 */
	public final void setInDoubt(boolean isWrite, int commandSentCounter) {
		if (isWrite && (commandSentCounter > 1 || (commandSentCounter == 1 && (resultCode == ResultCode.TIMEOUT || resultCode <= 0)))) {
			this.inDoubt = true;
		}
	}

	/**
	 * Exception thrown when database request expires before completing.
	 */
	public static final class Timeout extends AerospikeException {
		private static final long serialVersionUID = 1L;

		/**
		 * If true, client initiated timeout.  If false, server initiated timeout.
		 */
		public boolean client;

		public Timeout(String message, int iteration, int totalTimeout, boolean inDoubt) {
			super(ResultCode.TIMEOUT, message);
			super.totalTimeout = totalTimeout;
			super.iteration = iteration;
			super.inDoubt = inDoubt;
			this.client = true;
		}

		public Timeout(Command cmd, boolean client) {
			// Other base exception fields are set after this constructor.
			super(ResultCode.TIMEOUT, (client ? "Client" : "Server") + " timeout");
			super.setCommand(cmd);
			this.client = client;
		}

		public Timeout(Command cmd, int iteration) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.setCommand(cmd);
			super.iteration = iteration;
			this.client = true;
		}

		public Timeout(Node node, int connectTimeout, int socketTimeout, int totalTimeout) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.node = node;
			super.connectTimeout = connectTimeout;
			super.socketTimeout = socketTimeout;
			super.totalTimeout = totalTimeout;
			super.iteration = 1;
			this.client = true;
		}
	}

    /**
     * Security-related error from the server (authentication, authorization, roles, etc.).
     */
    public static class SecurityException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public SecurityException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Authentication failed: invalid user, password, credential, or session expired.
     */
    public static class AuthenticationException extends SecurityException {
        private static final long serialVersionUID = 1L;

        public AuthenticationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Authenticated user lacks the required role or is not whitelisted.
     */
    public static class AuthorizationException extends SecurityException {
        private static final long serialVersionUID = 1L;

        public AuthorizationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Record's generation (version) does not match the expected value. Indicates an
     * optimistic locking conflict -- another writer modified the record first.
     */
    public static class GenerationException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public GenerationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Quota limit exceeded or quota configuration error.
     */
    public static class QuotaException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public QuotaException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Record does not exist. Thrown on read, touch, update or replace of a non-existent record.
     */
    public static class RecordNotFoundException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public RecordNotFoundException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Record already exists. Thrown on create-only (insert) operations when the key is already present.
     */
    public static class RecordExistsException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public RecordExistsException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Operation skipped because the record's filter expression evaluated to false.
     */
    public static class FilteredException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public FilteredException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Record size exceeds the server's configured limit.
     */
    public static class RecordTooBigException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public RecordTooBigException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Bin-level error: name too long, wrong type, not found, already exists, or invalid operation.
     */
    public static class BinException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public BinException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Bin already exists on a create-only bin operation.
     */
    public static class BinExistsException extends BinException {
        private static final long serialVersionUID = 1L;

        public BinExistsException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Bin not found on an update-only bin operation.
     */
    public static class BinNotFoundException extends BinException {
        private static final long serialVersionUID = 1L;

        public BinNotFoundException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Operation is incompatible with the bin's current data type (e.g. arithmetic on a string bin).
     */
    public static class BinTypeException extends BinException {
        private static final long serialVersionUID = 1L;

        public BinTypeException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Operation cannot be applied to the bin's current value (e.g. list op on a non-list bin).
     */
    public static class BinOpInvalidException extends BinException {
        private static final long serialVersionUID = 1L;

        public BinOpInvalidException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * CDT (List/Map) element-level error: element not found or already exists.
     */
    public static class ElementException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public ElementException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Map key or list element not found in an update-only CDT write mode.
     */
    public static class ElementNotFoundException extends ElementException {
        private static final long serialVersionUID = 1L;

        public ElementNotFoundException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Map key or list element already exists in a create-only CDT write mode.
     */
    public static class ElementExistsException extends ElementException {
        private static final long serialVersionUID = 1L;

        public ElementExistsException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Multi-record transaction (MRT) error: blocked, expired, version mismatch,
     * write limit exceeded, or invalid transaction state.
     */
    public static class TransactionException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public TransactionException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }

        public TransactionException(int resultCode, String message) {
            super(resultCode, message);
        }

        public TransactionException(int resultCode, String message, Throwable cause) {
            super(resultCode, message, cause);
        }
    }

    /**
     * Server or client resource exhaustion: memory, connections, device I/O, or queue capacity.
     */
    public static class CapacityException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public CapacityException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Too many concurrent operations on the same record (hot key contention).
     */
    public static class KeyBusyException extends CapacityException {
        private static final long serialVersionUID = 1L;

        public KeyBusyException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Secondary index error: index not found, already exists, out of memory, or limit exceeded.
     */
    public static class IndexException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public IndexException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Server-side query or scan error: aborted, timed out, or queue full.
     */
    public static class QueryException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public QueryException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * Batch operation error: one or more keys failed, or batch functionality is disabled.
     */
    public static class BatchException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public BatchException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
     * A user-defined function (UDF) returned an error.
     */
    public static class UdfException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public UdfException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    /**
	 * Exception thrown when a serialization error occurs.
	 */
	public static final class Serialize extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Serialize(Throwable e) {
			super(ResultCode.SERIALIZE_ERROR, "Serialize error", e);
		}

		public Serialize(String message) {
			super(ResultCode.SERIALIZE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when client can't parse data returned from server.
	 */
	public static final class Parse extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Parse(String message) {
			super(ResultCode.PARSE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when client can't connect to the server.
	 */
	public static final class Connection extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Connection(String message) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message);
		}

		public Connection(Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, "Connection failed", e);
		}

		public Connection(String message, Throwable e) {
			super(ResultCode.SERVER_NOT_AVAILABLE, message, e);
		}

		public Connection(int resultCode, String message) {
			super(resultCode, message);
		}
	}

	/**
	 * Exception thrown when chosen node is not active.
	 */
	public static final class InvalidNode extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNode(int clusterSize, Partition partition) {
			super(ResultCode.INVALID_NODE_ERROR,
				(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition);
		}

		public InvalidNode(int partitionId) {
			super(ResultCode.INVALID_NODE_ERROR, "Node not found for partition " + partitionId);
		}

		public InvalidNode(String message) {
			super(ResultCode.INVALID_NODE_ERROR, message);
		}
	}

	/**
	 * Exception thrown when namespace is invalid.
	 */
	public static final class InvalidNamespace extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public InvalidNamespace(String ns, int mapSize) {
			super(ResultCode.INVALID_NAMESPACE,
				(mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns);
		}
	}

	/**
	 * Exception thrown when query was terminated prematurely.
	 */
	public static final class QueryTerminated extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public QueryTerminated() {
			super(ResultCode.QUERY_TERMINATED, "Query terminated");
		}

		public QueryTerminated(Throwable e) {
			super(ResultCode.QUERY_TERMINATED, "Query terminated", e);
		}
	}

	/**
	 * Exception thrown when node is in backoff mode due to excessive
	 * number of errors.
	 */
	public static class Backoff extends AerospikeException {
		private static final long serialVersionUID = 1L;

		public Backoff(int resultCode) {
			super(resultCode);
		}
	}

	/**
	 * Transaction commit failed. Contains verify and roll-forward/backward details.
	 */
	public static final class Commit extends TransactionException {
		private static final long serialVersionUID = 1L;

		/**
		 * Error status of the attempted commit.
		 */
		public final CommitError error;

		/**
		 * Verify result for each read key in the transaction. May be null if failure occurred
		 * before verify.
		 */
		public final List<BatchRecord> verifyRecords;

		/**
		 * Roll forward/backward result for each write key in the transaction. May be null if
		 * failure occurred before roll forward/backward.
		 */
		public final List<BatchRecord> rollRecords;

		public Commit(CommitError error, List<BatchRecord> verifyRecords, List<BatchRecord> rollRecords) {
			super(ResultCode.TXN_FAILED, error.str);
			this.error = error;
			this.verifyRecords = verifyRecords;
			this.rollRecords = rollRecords;
		}

		public Commit(CommitError error, List<BatchRecord> verifyRecords, List<BatchRecord> rollRecords, Throwable cause) {
			super(ResultCode.TXN_FAILED, error.str, cause);
			this.error = error;
			this.verifyRecords = verifyRecords;
			this.rollRecords = rollRecords;
		}

		@Override
		public String getMessage() {
			String msg = super.getMessage();
			StringBuilder sb = new StringBuilder(1024);
			recordsToString(sb, "verify errors:", verifyRecords);
			recordsToString(sb, "roll errors:", rollRecords);
			return msg + sb.toString();
		}
	}

	private static void recordsToString(StringBuilder sb, String title, List<BatchRecord> records) {
		if (records == null) {
			return;
		}

		int count = 0;

		for (BatchRecord br : records) {
			// Only show results with an error response.
			if (!(br.resultCode == ResultCode.OK || br.resultCode == ResultCode.NO_RESPONSE)) {
				// Only show first 3 errors.
				if (count >= 3) {
					sb.append(System.lineSeparator());
					sb.append("...");
					break;
				}

				if (count == 0) {
					sb.append(System.lineSeparator());
					sb.append(title);
				}

				sb.append(System.lineSeparator());
				sb.append(br.key);
				sb.append(',');
				sb.append(br.resultCode);
				sb.append(',');
				sb.append(br.inDoubt);
				count++;
			}
		}
	}

    /**
     * Map a server result code to the appropriate exception subclass.
     *
     * <pre>
     * Exception Hierarchy:
     *
     * AerospikeException
     * ├── RecordNotFoundException         KEY_NOT_FOUND_ERROR
     * ├── RecordExistsException           KEY_EXISTS_ERROR
     * ├── GenerationException             GENERATION_ERROR
     * ├── FilteredException               FILTERED_OUT
     * ├── RecordTooBigException           RECORD_TOO_BIG
     * ├── BinException                    BIN_NAME_TOO_LONG
     * │   ├── BinExistsException          BIN_EXISTS_ERROR
     * │   ├── BinNotFoundException        BIN_NOT_FOUND
     * │   ├── BinTypeException            BIN_TYPE_ERROR
     * │   └── BinOpInvalidException       OP_NOT_APPLICABLE
     * ├── ElementException
     * │   ├── ElementNotFoundException    ELEMENT_NOT_FOUND
     * │   └── ElementExistsException      ELEMENT_EXISTS
     * ├── TransactionException            TXN_ALREADY_ABORTED, TXN_ALREADY_COMMITTED,
     * │   │                               MRT_BLOCKED, MRT_VERSION_MISMATCH, MRT_EXPIRED,
     * │   │                               MRT_TOO_MANY_WRITES, MRT_COMMITTED, MRT_ABORTED,
     * │   │                               MRT_ALREADY_LOCKED, MRT_MONITOR_EXISTS
     * │   └── Commit                      TXN_FAILED
     * ├── SecurityException               ILLEGAL_STATE, USER_ALREADY_EXISTS, FORBIDDEN_PASSWORD,
     * │   │                               SECURITY_NOT_ENABLED, SECURITY_NOT_SUPPORTED,
     * │   │                               SECURITY_SCHEME_NOT_SUPPORTED, EXPIRED_SESSION,
     * │   │                               INVALID_ROLE, ROLE_ALREADY_EXISTS, INVALID_PRIVILEGE,
     * │   │                               INVALID_WHITELIST
     * │   ├── AuthenticationException     INVALID_USER, INVALID_PASSWORD, INVALID_CREDENTIAL,
     * │   │                               EXPIRED_PASSWORD, NOT_AUTHENTICATED
     * │   └── AuthorizationException      ROLE_VIOLATION, NOT_WHITELISTED
     * ├── QuotaException                  QUOTA_EXCEEDED, QUOTAS_NOT_ENABLED, INVALID_QUOTA
     * ├── CapacityException               SERVER_MEM_ERROR, DEVICE_OVERLOAD, NO_MORE_CONNECTIONS,
     * │   │                               ASYNC_QUEUE_FULL, BATCH_QUEUES_FULL,
     * │   │                               BATCH_MAX_REQUESTS_EXCEEDED
     * │   └── KeyBusyException            KEY_BUSY
     * ├── IndexException                  INDEX_ALREADY_EXISTS..INDEX_MAXCOUNT (200-206)
     * ├── QueryException                  QUERY_ABORTED, QUERY_QUEUEFULL, QUERY_TIMEOUT,
     * │                                   QUERY_GENERIC, SCAN_ABORT
     * ├── BatchException                  BATCH_FAILED, BATCH_DISABLED
     * ├── UdfException                    UDF_BAD_RESPONSE
     * ├── Timeout                         TIMEOUT
     * ├── Connection                      SERVER_NOT_AVAILABLE
     * ├── InvalidNode                     INVALID_NODE_ERROR
     * ├── Serialize                       SERIALIZE_ERROR
     * ├── Parse                           PARSE_ERROR
     * ├── InvalidNamespace                INVALID_NAMESPACE
     * ├── QueryTerminated                 QUERY_TERMINATED
     * └── Backoff                         MAX_ERROR_RATE
     * </pre>
     */
	public static AerospikeException resultCodeToException(int resultCode, String message, boolean inDoubt) {
        switch (resultCode) {

        // Record-level
        case ResultCode.KEY_NOT_FOUND_ERROR:
            return new RecordNotFoundException(resultCode, message, inDoubt);
        case ResultCode.KEY_EXISTS_ERROR:
            return new RecordExistsException(resultCode, message, inDoubt);
        case ResultCode.GENERATION_ERROR:
            return new GenerationException(resultCode, message, inDoubt);
        case ResultCode.FILTERED_OUT:
            return new FilteredException(resultCode, message, inDoubt);
        case ResultCode.RECORD_TOO_BIG:
            return new RecordTooBigException(resultCode, message, inDoubt);

        // Bin-level
        case ResultCode.BIN_EXISTS_ERROR:
            return new BinExistsException(resultCode, message, inDoubt);
        case ResultCode.BIN_NOT_FOUND:
            return new BinNotFoundException(resultCode, message, inDoubt);
        case ResultCode.BIN_TYPE_ERROR:
            return new BinTypeException(resultCode, message, inDoubt);
        case ResultCode.OP_NOT_APPLICABLE:
            return new BinOpInvalidException(resultCode, message, inDoubt);
        case ResultCode.BIN_NAME_TOO_LONG:
            return new BinException(resultCode, message, inDoubt);

        // CDT element-level
        case ResultCode.ELEMENT_NOT_FOUND:
            return new ElementNotFoundException(resultCode, message, inDoubt);
        case ResultCode.ELEMENT_EXISTS:
            return new ElementExistsException(resultCode, message, inDoubt);

        // Transaction / MRT
        case ResultCode.TXN_ALREADY_ABORTED:
        case ResultCode.TXN_ALREADY_COMMITTED:
        case ResultCode.MRT_BLOCKED:
        case ResultCode.MRT_VERSION_MISMATCH:
        case ResultCode.MRT_EXPIRED:
        case ResultCode.MRT_TOO_MANY_WRITES:
        case ResultCode.MRT_COMMITTED:
        case ResultCode.MRT_ABORTED:
        case ResultCode.MRT_ALREADY_LOCKED:
        case ResultCode.MRT_MONITOR_EXISTS:
            return new TransactionException(resultCode, message, inDoubt);

        // Security
        case ResultCode.INVALID_USER:
        case ResultCode.INVALID_PASSWORD:
        case ResultCode.INVALID_CREDENTIAL:
        case ResultCode.EXPIRED_PASSWORD:
        case ResultCode.NOT_AUTHENTICATED:
            return new AuthenticationException(resultCode, message, inDoubt);

        case ResultCode.ROLE_VIOLATION:
        case ResultCode.NOT_WHITELISTED:
            return new AuthorizationException(resultCode, message, inDoubt);

        case ResultCode.ILLEGAL_STATE:
        case ResultCode.USER_ALREADY_EXISTS:
        case ResultCode.FORBIDDEN_PASSWORD:
        case ResultCode.SECURITY_NOT_ENABLED:
        case ResultCode.SECURITY_NOT_SUPPORTED:
        case ResultCode.SECURITY_SCHEME_NOT_SUPPORTED:
        case ResultCode.EXPIRED_SESSION:
        case ResultCode.INVALID_ROLE:
        case ResultCode.ROLE_ALREADY_EXISTS:
        case ResultCode.INVALID_PRIVILEGE:
        case ResultCode.INVALID_WHITELIST:
            return new SecurityException(resultCode, message, inDoubt);

        // Quota
        case ResultCode.QUOTA_EXCEEDED:
        case ResultCode.QUOTAS_NOT_ENABLED:
        case ResultCode.INVALID_QUOTA:
            return new QuotaException(resultCode, message, inDoubt);

        // Capacity
        case ResultCode.KEY_BUSY:
            return new KeyBusyException(resultCode, message, inDoubt);
        case ResultCode.SERVER_MEM_ERROR:
        case ResultCode.DEVICE_OVERLOAD:
        case ResultCode.NO_MORE_CONNECTIONS:
        case ResultCode.ASYNC_QUEUE_FULL:
        case ResultCode.BATCH_QUEUES_FULL:
        case ResultCode.BATCH_MAX_REQUESTS_EXCEEDED:
            return new CapacityException(resultCode, message, inDoubt);

        // Index
        case ResultCode.INDEX_ALREADY_EXISTS:
        case ResultCode.INDEX_NOTFOUND:
        case ResultCode.INDEX_OOM:
        case ResultCode.INDEX_NOTREADABLE:
        case ResultCode.INDEX_GENERIC:
        case ResultCode.INDEX_NAME_MAXLEN:
        case ResultCode.INDEX_MAXCOUNT:
            return new IndexException(resultCode, message, inDoubt);

        // Query / Scan
        case ResultCode.QUERY_ABORTED:
        case ResultCode.QUERY_QUEUEFULL:
        case ResultCode.QUERY_TIMEOUT:
        case ResultCode.QUERY_GENERIC:
        case ResultCode.SCAN_ABORT:
            return new QueryException(resultCode, message, inDoubt);

        // Batch
        case ResultCode.BATCH_FAILED:
        case ResultCode.BATCH_DISABLED:
            return new BatchException(resultCode, message, inDoubt);

        // UDF
        case ResultCode.UDF_BAD_RESPONSE:
            return new UdfException(resultCode, message, inDoubt);

        default:
            return new AerospikeException(resultCode, message, inDoubt);
        }
    }
}
