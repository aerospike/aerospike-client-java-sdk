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
package com.aerospike.client.fluent.command;

import java.util.zip.Deflater;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.ReadModeAP;

public final class CommandBuffer {
	private byte[] dataBuffer;
	private int dataOffset;
	private Long version;

	//--------------------------------------------------
	// Operate
	//--------------------------------------------------

	public final void setOperate(OperateCommand cmd) {
		begin();
		int fieldCount = estimateKeySize(cmd, cmd.key, cmd.hasWrite);

		if (cmd.filterExp != null) {
			sizeFieldExpression(cmd.filterExp);
			fieldCount++;
		}
		dataOffset += cmd.size;
		sizeBuffer();

		writeHeaderOperate(cmd, fieldCount);
		writeKey(cmd, cmd.key, cmd.hasWrite);

		if (cmd.filterExp != null) {
			writeFieldExpression(cmd.filterExp);
		}

		for (Operation op : cmd.ops) {
			writeOperation(op);
		}
		end();
		compress(cmd);
	}

	//--------------------------------------------------
	// Transaction Monitor
	//--------------------------------------------------

	public final void setTxnAddKeys(OperateCommand cmd) {
		begin();
		int fieldCount = estimateKeySize(cmd.key);
		dataOffset += cmd.size;

		sizeBuffer();

		dataBuffer[8]  = Command.MSG_REMAINING_HEADER_SIZE;
		dataBuffer[9]  = (byte)cmd.readAttr;
		dataBuffer[10] = (byte)cmd.writeAttr;
		dataBuffer[11] = (byte)0;
		dataBuffer[12] = 0;
		dataBuffer[13] = 0;
		Buffer.intToBytes(0, dataBuffer, 14);
		Buffer.intToBytes(cmd.ttl, dataBuffer, 18);
		Buffer.intToBytes(cmd.serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(cmd.ops.length, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;

		writeKey(cmd.key);

		for (Operation op : cmd.ops) {
			writeOperation(op);
		}
		end();
		compress(cmd);
	}

	//--------------------------------------------------
	// Command Sizing
	//--------------------------------------------------

	private int estimateKeySize(Command cmd, Key key, boolean hasWrite) {
		int fieldCount = estimateKeySize(key);

		fieldCount += sizeTxn(cmd, key, hasWrite);

		if (cmd.sendKey) {
			dataOffset += key.userKey.estimateSize() + Command.FIELD_HEADER_SIZE + 1;
			fieldCount++;
		}
		return fieldCount;
	}

	private int estimateKeySize(Key key) {
		int fieldCount = 0;

		if (key.namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(key.namespace) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (key.setName != null) {
			dataOffset += Buffer.estimateSizeUtf8(key.setName) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		dataOffset += key.digest.length + Command.FIELD_HEADER_SIZE;
		fieldCount++;
		return fieldCount;
	}

	private int sizeTxn(Command cmd, Key key, boolean hasWrite) {
		int fieldCount = 0;

		if (cmd.txn != null) {
			dataOffset += 8 + Command.FIELD_HEADER_SIZE;
			fieldCount++;

			version = cmd.txn.getReadVersion(key);

			if (version != null) {
				dataOffset += 7 + Command.FIELD_HEADER_SIZE;
				fieldCount++;
			}

			if (hasWrite && cmd.txn.getDeadline() != 0) {
				dataOffset += 4 + Command.FIELD_HEADER_SIZE;
				fieldCount++;
			}
		}
		return fieldCount;
	}

	private void sizeFieldExpression(Expression exp) {
		byte[] bytes = exp.getBytes();
		dataOffset += bytes.length + Command.FIELD_HEADER_SIZE;
	}

	private void sizeBuffer() {
		dataBuffer = new byte[dataOffset];
	}

	//--------------------------------------------------
	// Command Writes
	//--------------------------------------------------

	private final void writeHeaderOperate(OperateCommand cmd, int fieldCount) {
		// Set flags.
		int gen = 0;
		int ttl = cmd.hasWrite ? cmd.ttl : cmd.readTouchTtlPercent;
		int readAttr = cmd.readAttr;
		int writeAttr = cmd.writeAttr;
		int infoAttr = 0;
		int txnAttr = 0;
		int operationCount = cmd.ops.length;

		switch (cmd.type) {
		default:
		case UPSERT:
			break;

		case INSERT:
			writeAttr |= Command.INFO2_CREATE_ONLY;
			break;

		case UPDATE:
			infoAttr |= Command.INFO3_UPDATE_ONLY;
			break;

		case REPLACE:
			infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
			break;
		}

		if (cmd.gen > 0) {
			gen = cmd.gen;
			writeAttr |= Command.INFO2_GENERATION;
		}

		if (cmd.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}

		if (cmd.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (cmd.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		switch (cmd.readModeSC) {
		case SESSION:
			break;
		case LINEARIZE:
			infoAttr |= Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr |= Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr |= Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}

		if (cmd.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		if (cmd.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8]  = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		dataBuffer[12] = (byte)txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(gen, dataBuffer, 14);
		Buffer.intToBytes(ttl, dataBuffer, 18);
		Buffer.intToBytes(cmd.serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
	}

	private void writeKey(Command cmd, Key key, boolean sendDeadline) {
		writeKey(key);
		writeTxn(cmd.txn, sendDeadline);

		if (cmd.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private void writeKey(Key key) {
		// Write key into buffer.
		writeField(key.namespace, FieldType.NAMESPACE);

		if (key.setName != null) {
			writeField(key.setName, FieldType.TABLE);
		}

		writeField(key.digest, FieldType.DIGEST_RIPE);
	}

	private void writeTxn(Txn txn, boolean sendDeadline) {
		if (txn != null) {
			writeFieldLE(txn.getId(), FieldType.TXN_ID);

			if (version != null) {
				writeFieldVersion(version);
			}

			if (sendDeadline && txn.getDeadline() != 0) {
				writeFieldLE(txn.getDeadline(), FieldType.TXN_DEADLINE);
			}
		}
	}

	private void writeFieldExpression(Expression exp) {
		byte[] bytes = exp.getBytes();
		writeFieldHeader(bytes.length, FieldType.FILTER_EXP);
		System.arraycopy(bytes, 0, dataBuffer, dataOffset, bytes.length);
		dataOffset += bytes.length;
	}

	private void writeFieldVersion(long ver) {
		writeFieldHeader(7, FieldType.RECORD_VERSION);
		Buffer.longToVersionBytes(ver, dataBuffer, dataOffset);
		dataOffset += 7;
	}

	private void writeFieldLE(long val, int type) {
		writeFieldHeader(8, type);
		Buffer.longToLittleBytes(val, dataBuffer, dataOffset);
		dataOffset += 8;
	}

	private void writeField(Value value, int type) {
		int offset = dataOffset + Command.FIELD_HEADER_SIZE;
		dataBuffer[offset++] = (byte)value.getType();
		int len = value.write(dataBuffer, offset) + 1;
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private void writeField(String str, int type) {
		int len = Buffer.stringToUtf8(str, dataBuffer, dataOffset + Command.FIELD_HEADER_SIZE);
		writeFieldHeader(len, type);
		dataOffset += len;
	}

	private void writeField(byte[] bytes, int type) {
		System.arraycopy(bytes, 0, dataBuffer, dataOffset + Command.FIELD_HEADER_SIZE, bytes.length);
		writeFieldHeader(bytes.length, type);
		dataOffset += bytes.length;
	}

	private void writeFieldHeader(int size, int type) {
		Buffer.intToBytes(size+1, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte)type;
	}

	private void writeOperation(Operation operation) {
		int nameLength = Buffer.stringToUtf8(operation.binName, dataBuffer, dataOffset + Command.OPERATION_HEADER_SIZE);
		int valueLength = operation.value.write(dataBuffer, dataOffset + Command.OPERATION_HEADER_SIZE + nameLength);

		Buffer.intToBytes(nameLength + valueLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.type.protocolType;
		dataBuffer[dataOffset++] = (byte) operation.value.getType();
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) nameLength;
		dataOffset += nameLength + valueLength;
	}

	//--------------------------------------------------
	// Command begin/end
	//--------------------------------------------------

	private void begin() {
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
	}

	private void end() {
		// Write total size of message which is the current offset.
		long proto = (dataOffset - 8) | (Command.CL_MSG_VERSION << 56) | (Command.AS_MSG_TYPE << 48);
		Buffer.longToBytes(proto, dataBuffer, 0);
	}

	//--------------------------------------------------
	// Command compress
	//--------------------------------------------------

	private final void compress(Command cmd) {
		if (cmd.compress && dataOffset > Command.COMPRESS_THRESHOLD) {
			Deflater def = new Deflater(Deflater.BEST_SPEED);
			try {
				def.setInput(dataBuffer, 0, dataOffset);
				def.finish();

				byte[] cbuf = new byte[dataOffset];
				int csize = def.deflate(cbuf, 16, dataOffset - 16);

				// Use compressed buffer if compression completed within original buffer size.
				if (def.finished()) {
					long proto = (csize + 8) | (Command.CL_MSG_VERSION << 56) | (Command.MSG_TYPE_COMPRESSED << 48);
					Buffer.longToBytes(proto, cbuf, 0);
					Buffer.longToBytes(dataOffset, cbuf, 8);
					dataBuffer = cbuf;
					dataOffset = csize + 16;
				}
			} finally {
				def.end();
			}
		}
	}

	//--------------------------------------------------
	// Getters
	//--------------------------------------------------

	public byte[] getBuffer() {
		return dataBuffer;
	}

	public int getLength() {
		return dataOffset;
	}
}
