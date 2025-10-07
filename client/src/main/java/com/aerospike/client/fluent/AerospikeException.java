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

import com.aerospike.client.fluent.policy.SettablePolicy;

public class AerospikeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    protected transient Node node;
    protected transient SettablePolicy policy;
    protected List<AerospikeException> subExceptions;
    protected int resultCode = ResultCode.CLIENT_ERROR;
    protected int iteration = -1;
    protected boolean inDoubt;

    public AerospikeException(String message) {
        super(message);
    }

    public AerospikeException(String message, Throwable e) {
        super(message, e);
    }

    public AerospikeException(int resultCode, String message, boolean inDoubt) {
        super(message);
        this.resultCode = resultCode;
        this.inDoubt = inDoubt;
    }

    public AerospikeException(int resultCode, String message, Throwable e) {
        super(message, e);
        this.resultCode = resultCode;
    }

    public AerospikeException(int resultCode, String message) {
        super(message);
        this.resultCode = resultCode;
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

		if (policy != null) {
			sb.append(',');
			sb.append(policy.getWaitForConnectionToComplete());
			sb.append(',');
			sb.append(policy.getWaitForCallToComplete());
			sb.append(',');
			sb.append(policy.getAbandonCallAfter());
			sb.append(',');
			sb.append(policy.getMaximumNumberOfCallAttempts());
		}

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
	 * Is it possible that write command may have completed.
	 */
	public final boolean getInDoubt() {
		return inDoubt;
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
			super.iteration = iteration;
			super.inDoubt = inDoubt;

			/*
			Policy p = new Policy();
			p.connectTimeout = 0;
			p.socketTimeout = 0;
			p.totalTimeout = totalTimeout;
			p.maxRetries = -1;
			super.policy = p;
			*/
			this.client = true;
		}
/*
		public Timeout(Policy policy, boolean client) {
			// Other base exception fields are set after this constructor.
			super(ResultCode.TIMEOUT, (client ? "Client" : "Server") + " timeout");
			super.policy = policy;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = client;
		}

		public Timeout(Policy policy, int iteration) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.policy = policy;
			super.iteration = iteration;
			this.connectTimeout = policy.connectTimeout;
			this.socketTimeout = policy.socketTimeout;
			this.timeout = policy.totalTimeout;
			this.client = true;
		}
*/
		public Timeout(Node node, int connectTimeout, int socketTimeout, int totalTimeout) {
			super(ResultCode.TIMEOUT, "Client timeout");
			super.node = node;
			super.iteration = 1;

			/*
			Policy p = new Policy();
			p.connectTimeout = connectTimeout;
			p.socketTimeout = socketTimeout;
			p.totalTimeout = totalTimeout;
			p.maxRetries = 0;
			super.policy = p;
			*/
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

		/*
		public InvalidNode(int clusterSize, Partition partition) {
			super(ResultCode.INVALID_NODE_ERROR,
				(clusterSize == 0) ? "Cluster is empty" : "Node not found for partition " + partition);
		}*/

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
