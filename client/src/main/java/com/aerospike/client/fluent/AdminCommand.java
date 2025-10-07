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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.mindrot.jbcrypt.BCrypt;

import com.aerospike.client.fluent.policy.AuthMode;

public class AdminCommand {
	// Commands
	private static final byte AUTHENTICATE = 0;
	/*
	private static final byte CREATE_USER = 1;
	private static final byte DROP_USER = 2;
	private static final byte SET_PASSWORD = 3;
	private static final byte CHANGE_PASSWORD = 4;
	private static final byte GRANT_ROLES = 5;
	private static final byte REVOKE_ROLES = 6;
	private static final byte QUERY_USERS = 9;
	private static final byte CREATE_ROLE = 10;
	private static final byte DROP_ROLE = 11;
	private static final byte GRANT_PRIVILEGES = 12;
	private static final byte REVOKE_PRIVILEGES = 13;
	private static final byte SET_WHITELIST = 14;
	private static final byte SET_QUOTAS = 15;
	private static final byte QUERY_ROLES = 16;
	*/
	private static final byte LOGIN = 20;

	// Field IDs
	private static final byte USER = 0;
	/*
	private static final byte PASSWORD = 1;
	private static final byte OLD_PASSWORD = 2;
	*/
	private static final byte CREDENTIAL = 3;
	private static final byte CLEAR_PASSWORD = 4;
	private static final byte SESSION_TOKEN = 5;
	private static final byte SESSION_TTL = 6;
	/*
	private static final byte ROLES = 10;
	private static final byte ROLE = 11;
	private static final byte PRIVILEGES = 12;
	private static final byte WHITELIST = 13;
	private static final byte READ_QUOTA = 14;
	private static final byte WRITE_QUOTA = 15;
	private static final byte READ_INFO = 16;
	private static final byte WRITE_INFO = 17;
	private static final byte CONNECTIONS = 18;
	*/

	// Misc
	private static final long MSG_VERSION = 2L;
	private static final long MSG_TYPE = 2L;
	private static final int FIELD_HEADER_SIZE = 5;
	private static final int HEADER_SIZE = 24;
	private static final int HEADER_REMAINING = 16;
	private static final int RESULT_CODE = 9;
	//private static final int QUERY_END = 50;

	byte[] dataBuffer;
	int dataOffset;

	public AdminCommand() {
		this.dataBuffer = new byte[8192];
		dataOffset = 8;
	}

	public AdminCommand(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
		dataOffset = 8;
	}

	public static class LoginCommand extends AdminCommand {
		public byte[] sessionToken = null;
		public long sessionExpiration = 0;

		public LoginCommand(ClusterDefinition def, Connection conn) throws IOException {
			super();

			conn.setTimeout(def.loginTimeout);

			try {
				login(def, conn);
			}
			finally {
				conn.setTimeout(def.tendTimeout);
			}
		}

		private void login(ClusterDefinition def, Connection conn) throws IOException {
			if (def.authMode == AuthMode.INTERNAL) {
				writeHeader(LOGIN, 2);
				writeField(USER, def.userName);
				writeField(CREDENTIAL, def.passwordHash);
			}
			else if (def.authMode == AuthMode.PKI) {
				writeHeader(LOGIN, 0);
			}
			else {
				writeHeader(LOGIN, 3);
				writeField(USER, def.userName);
				writeField(CREDENTIAL, def.passwordHash);
				writeField(CLEAR_PASSWORD, def.password);
			}
			writeSize();
			conn.write(dataBuffer, dataOffset);
			conn.readFully(dataBuffer, HEADER_SIZE);

			int result = dataBuffer[RESULT_CODE] & 0xFF;

			if (result != 0) {
				if (result == ResultCode.SECURITY_NOT_ENABLED) {
					// Server does not require login.
					return;
				}
				String msg = "Login failed";
				if (result == ResultCode.INVALID_CREDENTIAL) {
					msg = "Authentication failed: Password authentication is disabled for PKI-only users. " +
							"Please authenticate using your certificate.";
				}

				throw new AerospikeException(result, msg);
			}

			// Read session token.
			long size = Buffer.bytesToLong(dataBuffer, 0);
			int receiveSize = ((int)(size & 0xFFFFFFFFFFFFL)) - HEADER_REMAINING;
			int fieldCount = dataBuffer[11] & 0xFF;

			if (receiveSize <= 0 || receiveSize > dataBuffer.length || fieldCount <= 0) {
				throw new AerospikeException(result, "Failed to retrieve session token");
			}

			conn.readFully(dataBuffer, receiveSize);
			dataOffset = 0;

			for (int i = 0; i < fieldCount; i++) {
				int len = Buffer.bytesToInt(dataBuffer, dataOffset);
				dataOffset += 4;
				int id = dataBuffer[dataOffset++] & 0xFF;
				len--;

				if (id == SESSION_TOKEN) {
					sessionToken = Arrays.copyOfRange(dataBuffer, dataOffset, dataOffset + len);
				}
				else if (id == SESSION_TTL) {
					// Subtract 60 seconds from ttl so client session expires before server session.
					long seconds = Buffer.bigUnsigned32ToLong(dataBuffer, dataOffset) - 60;

					if (seconds > 0) {
						sessionExpiration = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
					}
					else {
						if (Log.warnEnabled()) {
							Log.warn("Invalid session TTL: " + seconds);
						}
					}
				}
				dataOffset += len;
			}

			if (sessionToken == null) {
				throw new AerospikeException(result, "Failed to retrieve session token");
			}
		}
	}

	public static boolean authenticate(ClusterDefinition def, Connection conn, byte[] sessionToken)
		throws IOException {
		AdminCommand command = new AdminCommand();
		return command.authenticateSession(def, conn, sessionToken);
	}

	private boolean authenticateSession(ClusterDefinition def, Connection conn, byte[] sessionToken)
		throws IOException {

		dataOffset = 8;
		setAuthenticate(def, sessionToken);
		conn.write(dataBuffer, dataOffset);
		conn.readFully(dataBuffer, HEADER_SIZE, Command.STATE_READ_AUTH_HEADER);

		int result = dataBuffer[RESULT_CODE] & 0xFF;
		return result == 0 || result == ResultCode.SECURITY_NOT_ENABLED;
	}

	public int setAuthenticate(ClusterDefinition def, byte[] sessionToken) {
		if (def.authMode != AuthMode.PKI) {
			writeHeader(AUTHENTICATE, 2);
			writeField(USER, def.userName);
		}
		else {
			writeHeader(AUTHENTICATE, 1);
		}
		writeField(SESSION_TOKEN, sessionToken);
		writeSize();
		return dataOffset;
	}

	public static String hashPassword(String password) {
		return BCrypt.hashpw(password, "$2a$10$7EqJtq98hPqEX7fNZaFWoO");
	}

	final void writeSize() {
		// Write total size of message which is the current offset.
		long size = ((long)dataOffset - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
		Buffer.longToBytes(size, dataBuffer, 0);
	}

	final void writeHeader(byte command, int fieldCount) {
		// Authenticate header is almost all zeros
		Arrays.fill(dataBuffer, dataOffset, dataOffset + 16, (byte)0);
		dataBuffer[dataOffset + 2] = command;
		dataBuffer[dataOffset + 3] = (byte)fieldCount;
		dataOffset += 16;
	}

	/*
	private void writeField(byte id, String str) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + FIELD_HEADER_SIZE);
		writeFieldHeader(id, len);
		dataOffset += len;
	}
	*/

	final void writeField(byte id, byte[] bytes) {
		System.arraycopy(bytes, 0, dataBuffer, dataOffset + FIELD_HEADER_SIZE, bytes.length);
		writeFieldHeader(id, bytes.length);
		dataOffset += bytes.length;
	}

	/*
	private void writeField(byte id, int val) {
		writeFieldHeader(id, 4);
		Buffer.intToBytes(val, dataBuffer, dataOffset);
		dataOffset += 4;
	}
	*/

	private void writeFieldHeader(byte id, int size) {
		Buffer.intToBytes(size + 1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = id;
	}

	// TODO: Port all methods
}
