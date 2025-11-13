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
package com.aerospike.client.fluent;

import java.util.List;

import com.aerospike.client.fluent.command.BatchRecord;
import com.aerospike.client.fluent.command.Command;

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

    public static class SecurityException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public SecurityException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    public static class AuthenticationException extends SecurityException {
        private static final long serialVersionUID = 1L;

        public AuthenticationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    public static class AuthorizationException extends SecurityException {
        private static final long serialVersionUID = 1L;

        public AuthorizationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    public static class GenerationException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public GenerationException(int resultCode, String message, boolean inDoubt) {
            super(resultCode, message, inDoubt);
        }
    }

    public static class QuotaException extends AerospikeException {
        private static final long serialVersionUID = 1L;

        public QuotaException(int resultCode, String message, boolean inDoubt) {
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
	 * Exception thrown when a transaction commit fails.
	 */
	public static final class Commit extends AerospikeException {
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

	public static AerospikeException resultCodeToException(int resultCode, String message, boolean inDoubt) {
        switch (resultCode) {
        case ResultCode.QUOTA_EXCEEDED:
        case ResultCode.QUOTAS_NOT_ENABLED:
        case ResultCode.INVALID_QUOTA:
            return new QuotaException(resultCode, message, inDoubt);

        case ResultCode.INVALID_USER:
        case ResultCode.INVALID_PASSWORD:
        case ResultCode.INVALID_CREDENTIAL:
        case ResultCode.EXPIRED_PASSWORD:
        case ResultCode.NOT_AUTHENTICATED:
            return new AuthenticationException(resultCode, message, inDoubt);

        case ResultCode.ROLE_VIOLATION:
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

        case ResultCode.GENERATION_ERROR:
            return new GenerationException(resultCode, message, inDoubt);

        default:
            return new AerospikeException(resultCode, message, inDoubt);
        }
    }
}
