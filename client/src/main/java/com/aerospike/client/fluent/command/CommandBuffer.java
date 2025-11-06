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

import com.aerospike.client.fluent.AerospikeException;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.ResultCode;
import com.aerospike.client.fluent.Txn;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.command.PartitionTracker.NodePartitions;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.BatchDeletePolicy;
import com.aerospike.client.fluent.policy.BatchReadPolicy;
import com.aerospike.client.fluent.policy.BatchUDFPolicy;
import com.aerospike.client.fluent.policy.BatchWritePolicy;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.QueryDuration;
import com.aerospike.client.fluent.policy.ReadModeAP;

public final class CommandBuffer {
	public static final byte BATCH_MSG_READ = 0x0;
	public static final byte BATCH_MSG_REPEAT = 0x1;
	public static final byte BATCH_MSG_INFO = 0x2;
	public static final byte BATCH_MSG_GEN = 0x4;
	public static final byte BATCH_MSG_TTL = 0x8;
	public static final byte BATCH_MSG_INFO4 = 0x10;

	private byte[] dataBuffer;
	private int dataOffset;
	private Long version;

	//--------------------------------------------------
	// Operate
	//--------------------------------------------------

	public void setOperate(OperateWriteCommand cmd) {
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

	public void setOperate(BatchCommand cmd, BatchAttr attr, Key key, Operation[] ops) {
		begin();
		Expression exp = getBatchExpression(cmd, attr);
		int fieldCount = estimateKeyAttrSize(cmd, key, attr, exp);

		dataOffset += attr.opSize;
		sizeBuffer();
		writeKeyAttr(cmd, key, attr, exp, fieldCount, ops.length);

		for (Operation op : ops) {
			writeOperation(op);
		}
		end();
		compress(cmd);
	}

	//--------------------------------------------------
	// Read
	//--------------------------------------------------

	public void setRead(ReadCommand cmd) {
		int readAttr = Command.INFO1_READ;
		int opCount = 0;

		if (cmd.withNoBins) {
			readAttr |= Command.INFO1_NOBINDATA;
		}
		else if (cmd.binNames != null && cmd.binNames.length > 0) {
			opCount = cmd.binNames.length;
		}
		else {
			readAttr |= Command.INFO1_GET_ALL;
		}

		begin();
		int fieldCount = estimateKeySize(cmd, cmd.key, false);

		if (cmd.filterExp != null) {
			sizeFieldExpression(cmd.filterExp);
			fieldCount++;
		}

		if (opCount != 0) {
			for (String binName : cmd.binNames) {
				estimateOperationSize(binName);
			}
		}

		sizeBuffer();
		writeHeaderRead(cmd, cmd.serverTimeout, readAttr, 0, 0, fieldCount, opCount);
		writeKey(cmd, cmd.key, false);

		if (cmd.filterExp != null) {
			writeFieldExpression(cmd.filterExp);
		}

		if (opCount != 0) {
			for (String binName : cmd.binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		end();
	}

	public void setRead(BatchReadCommand cmd, BatchRead br) {
		begin();

		BatchReadPolicy rp = br.policy;
		BatchAttr attr = new BatchAttr();
		Expression exp;
		int opCount;

		if (rp != null) {
			attr.setRead(rp);
			exp = (rp.filterExp != null) ? rp.filterExp : cmd.filterExp;
		}
		else {
			attr.setRead(cmd);
			exp = cmd.filterExp;
		}

		if (br.binNames != null) {
			opCount = br.binNames.length;

			for (String binName : br.binNames) {
				estimateOperationSize(binName);
			}
		}
		else if (br.ops != null) {
			attr.adjustRead(br.ops);
			opCount = br.ops.length;

			for (Operation op : br.ops) {
				if (op.type.isWrite) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Write operations not allowed in read");
				}
				estimateOperationSize(op);
			}
		}
		else {
			attr.adjustRead(br.readAllBins);
			opCount = 0;
		}

		int fieldCount = estimateKeyAttrSize(cmd, br.key, attr, exp);

		sizeBuffer();
		writeKeyAttr(cmd, br.key, attr, exp, fieldCount, opCount);

		if (br.binNames != null) {
			for (String binName : br.binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}
		else if (br.ops != null) {
			for (Operation op : br.ops) {
				writeOperation(op);
			}
		}
		end();
	}

	//--------------------------------------------------
	// Batch Read/Write Operations
	//--------------------------------------------------

	public final void setBatchOperate(
		BatchCommand cmd,
		BatchNode batch,
		BatchWritePolicy writePolicy,
		BatchUDFPolicy udfPolicy,
		BatchDeletePolicy deletePolicy
	) {
		begin();
		int max = batch.offsetsSize;
		Txn txn = cmd.txn;
		Long[] versions = null;

		if (txn != null) {
			versions = new Long[max];

			for (int i = 0; i < max; i++) {
				int offset = batch.offsets[i];
				BatchRecord record = cmd.records.get(offset);
				versions[i] = txn.getReadVersion(record.key);
			}
		}

		int fieldCount = 1;

		if (cmd.filterExp != null) {
			sizeFieldExpression(cmd.filterExp);
			fieldCount++;
		}

		dataOffset += Command.FIELD_HEADER_SIZE + 5;

		BatchRecord prev = null;
		Long verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			BatchRecord record = cmd.records.get(offset);
			Key key = record.key;
			Long ver = (versions != null)? versions[i] : null;

			dataOffset += key.digest.length + 4;

			if (canRepeat(cmd, key, record, prev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataOffset++;
			}
			else {
				// Estimate full header, namespace and bin names.
				dataOffset += 12;
				dataOffset += Buffer.estimateSizeUtf8(key.namespace) + Command.FIELD_HEADER_SIZE;
				dataOffset += Buffer.estimateSizeUtf8(key.setName) + Command.FIELD_HEADER_SIZE;
				sizeTxnBatch(txn, ver, record.hasWrite);
				dataOffset += record.size(cmd);
				prev = record;
				verPrev = ver;
			}
		}
		sizeBuffer();

		writeBatchHeader(cmd, fieldCount);

		if (cmd.filterExp != null) {
			writeFieldExpression(cmd.filterExp);
		}

		final int fieldSizeOffset = dataOffset;
		writeFieldHeader(0, FieldType.BATCH_INDEX);  // Need to update size at end

		Buffer.intToBytes(max, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = getBatchFlags(cmd);

		BatchAttr attr = new BatchAttr();
		prev = null;
		verPrev = null;

		for (int i = 0; i < max; i++) {
			int offset = batch.offsets[i];
			BatchRecord record = cmd.records.get(offset);
			Long ver = (versions != null)? versions[i] : null;

			Buffer.intToBytes(offset, dataBuffer, dataOffset);
			dataOffset += 4;

			Key key = record.key;
			final byte[] digest = key.digest;
			System.arraycopy(digest, 0, dataBuffer, dataOffset, digest.length);
			dataOffset += digest.length;

			if (canRepeat(cmd, key, record, prev, ver, verPrev)) {
				// Can set repeat previous namespace/bin names to save space.
				dataBuffer[dataOffset++] = BATCH_MSG_REPEAT;
			}
			else {
				// Write full message.
				switch (record.getType()) {
					case BATCH_READ: {
						BatchRead br = (BatchRead)record;

						if (br.policy != null) {
							attr.setRead(br.policy);
						}
						else {
							attr.setRead((BatchReadCommand)cmd);
						}

						if (br.binNames != null) {
							if (br.binNames.length > 0) {
								writeBatchBinNames(key, txn, ver, br.binNames, attr, attr.filterExp);
							}
							else {
								attr.adjustRead(true);
								writeBatchRead(key, txn, ver, attr, attr.filterExp, 0);
							}
						}
						else if (br.ops != null) {
							attr.adjustRead(br.ops);
							writeBatchOperations(key, txn, ver, br.ops, attr, attr.filterExp);
						}
						else {
							attr.adjustRead(br.readAllBins);
							writeBatchRead(key, txn, ver, attr, attr.filterExp, 0);
						}
						break;
					}

					case BATCH_WRITE: {
						BatchWrite bw = (BatchWrite)record;
						BatchWritePolicy bwp = (bw.policy != null)? bw.policy : writePolicy;

						attr.setWrite(bwp);
						attr.adjustWrite(bw.ops);
						writeBatchOperations(key, txn, ver, bw.ops, attr, attr.filterExp);
						break;
					}

					case BATCH_UDF: {
						BatchUDF bu = (BatchUDF)record;
						BatchUDFPolicy bup = (bu.policy != null)? bu.policy : udfPolicy;

						attr.setUDF(bup);
						writeBatchWrite(key, txn, ver, attr, attr.filterExp, 3, 0);
						writeField(bu.packageName, FieldType.UDF_PACKAGE_NAME);
						writeField(bu.functionName, FieldType.UDF_FUNCTION);
						writeField(bu.argBytes, FieldType.UDF_ARGLIST);
						break;
					}

					case BATCH_DELETE: {
						BatchDelete bd = (BatchDelete)record;
						BatchDeletePolicy bdp = (bd.policy != null)? bd.policy : deletePolicy;

						attr.setDelete(bdp);
						writeBatchWrite(key, txn, ver, attr, attr.filterExp, 0, 0);
						break;
					}
				}
				prev = record;
				verPrev = ver;
			}
		}

		// Write real field size.
		Buffer.intToBytes(dataOffset - Command.MSG_TOTAL_HEADER_SIZE - 4, dataBuffer, fieldSizeOffset);
		end();
		compress(cmd);
	}

	private static byte getBatchFlags(BatchCommand cmd) {
		byte flags = 0x8;

		if (cmd.inlineMemory) {
			flags |= 0x1;
		}

		if (cmd.inlineSSD) {
			flags |= 0x2;
		}

		if (cmd.respondAllKeys) {
			flags |= 0x4;
		}
		return flags;
	}

	private static final Expression getBatchExpression(BatchCommand cmd, BatchAttr attr) {
		return (attr.filterExp != null) ? attr.filterExp : cmd.filterExp;
	}

	private static boolean canRepeat(
		Command cmd,
		Key key,
		BatchRecord record,
		BatchRecord prev,
		Long ver,
		Long verPrev
	) {
		// Avoid relatively expensive full equality checks for performance reasons.
		// Use reference equality only in hope that common namespaces/bin names are set from
		// fixed variables.  It's fine if equality not determined correctly because it just
		// results in more space used. The batch will still be correct.
		// Same goes for ver reference equality check.
		if (!(verPrev == ver && prev != null && prev.key.namespace == key.namespace &&
			prev.key.setName == key.setName)) {
			return false;
		}

		if (cmd.sendKey) {
			return false;
		}

		return record.equals(prev);
	}

	private void sizeTxnBatch(Txn txn, Long ver, boolean hasWrite) {
		if (txn != null) {
			dataOffset++; // Add info4 byte for transaction.
			dataOffset += 8 + Command.FIELD_HEADER_SIZE;

			if (ver != null) {
				dataOffset += 7 + Command.FIELD_HEADER_SIZE;
			}

			if (hasWrite && txn.getDeadline() != 0) {
				dataOffset += 4 + Command.FIELD_HEADER_SIZE;
			}
		}
	}

	private void writeBatchHeader(Command cmd, int fieldCount) {
		int readAttr = Command.INFO1_BATCH;

		if (cmd.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)0;
		dataBuffer[11] = (byte)0;

		for (int i = 12; i < 22; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(cmd.serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(0, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
	}

	private void writeBatchBinNames(Key key, Txn txn, Long ver, String[] binNames, BatchAttr attr, Expression filter) {
		writeBatchRead(key, txn, ver, attr, filter, binNames.length);

		for (String binName : binNames) {
			writeOperation(binName, Operation.Type.READ);
		}
	}

	private void writeBatchOperations(Key key, Txn txn, Long ver, Operation[] ops, BatchAttr attr, Expression filter) {
		if (attr.hasWrite) {
			writeBatchWrite(key, txn, ver, attr, filter, 0, ops.length);
		}
		else {
			writeBatchRead(key, txn, ver, attr, filter, ops.length);
		}

		for (Operation op : ops) {
			writeOperation(op);
		}
	}

	private void writeBatchRead(Key key, Txn txn, Long ver, BatchAttr attr, Expression filter, int opCount) {
		if (txn != null) {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataBuffer[dataOffset++] = (byte)attr.txnAttr;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsTxn(key, txn, ver, attr, filter, 0, opCount);
		}
		else {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsReg(key, attr, filter, 0, opCount);
		}
	}

	private void writeBatchWrite(
		Key key, Txn txn, Long ver, BatchAttr attr, Expression filter, int fieldCount, int opCount
	) {
		if (txn != null) {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_GEN | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			dataBuffer[dataOffset++] = (byte)attr.txnAttr;
			Buffer.shortToBytes(attr.generation, dataBuffer, dataOffset);
			dataOffset += 2;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsTxn(key, txn, ver, attr, filter, fieldCount, opCount);
		}
		else {
			dataBuffer[dataOffset++] = (byte)(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
			dataBuffer[dataOffset++] = (byte)attr.readAttr;
			dataBuffer[dataOffset++] = (byte)attr.writeAttr;
			dataBuffer[dataOffset++] = (byte)attr.infoAttr;
			Buffer.shortToBytes(attr.generation, dataBuffer, dataOffset);
			dataOffset += 2;
			Buffer.intToBytes(attr.expiration, dataBuffer, dataOffset);
			dataOffset += 4;
			writeBatchFieldsReg(key, attr, filter, fieldCount, opCount);
		}
	}

	private void writeBatchFieldsTxn(
		Key key, Txn txn, Long ver, BatchAttr attr, Expression filter, int fieldCount, int opCount
	) {
		fieldCount++;

		if (ver != null) {
			fieldCount++;
		}

		if (attr.hasWrite && txn.getDeadline() != 0) {
			fieldCount++;
		}

		if (filter != null) {
			fieldCount++;
		}

		if (attr.sendKey) {
			fieldCount++;
		}

		writeBatchFields(key, fieldCount, opCount);

		writeFieldLE(txn.getId(), FieldType.TXN_ID);

		if (ver != null) {
			writeFieldVersion(ver);
		}

		if (attr.hasWrite && txn.getDeadline() != 0) {
			writeFieldLE(txn.getDeadline(), FieldType.TXN_DEADLINE);
		}

		//if (filter != null) {
		//	filter.write(this);
		//}

		if (attr.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private void writeBatchFieldsReg(
		Key key, BatchAttr attr, Expression filter, int fieldCount, int opCount
	) {
		if (filter != null) {
			fieldCount++;
		}

		if (attr.sendKey) {
			fieldCount++;
		}

		writeBatchFields(key, fieldCount, opCount);

		//if (filter != null) {
		//	filter.write(this);
		//}

		if (attr.sendKey) {
			writeField(key.userKey, FieldType.KEY);
		}
	}

	private void writeBatchFields(Key key, int fieldCount, int opCount) {
		fieldCount += 2;
		Buffer.shortToBytes(fieldCount, dataBuffer, dataOffset);
		dataOffset += 2;
		Buffer.shortToBytes(opCount, dataBuffer, dataOffset);
		dataOffset += 2;
		writeField(key.namespace, FieldType.NAMESPACE);
		writeField(key.setName, FieldType.TABLE);
	}

	//--------------------------------------------------
	// Query
	//--------------------------------------------------

	public void setQuery(
		QueryCommand cmd, PartitionTracker tracker, NodePartitions nodePartitions, long taskId
	) {
		int fieldCount = 0;
		//int filterSize = 0;
		//int binNameSize = 0;

		begin();

		if (cmd.namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(cmd.namespace) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (cmd.set != null) {
			dataOffset += Buffer.estimateSizeUtf8(cmd.set) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate recordsPerSecond field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (cmd.recordsPerSecond > 0) {
			dataOffset += 4 + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate socket timeout field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		dataOffset += 4 + Command.FIELD_HEADER_SIZE;
		fieldCount++;

		// Estimate taskId field.
		dataOffset += 8 + Command.FIELD_HEADER_SIZE;
		fieldCount++;

		// TODO: Handle secondary index query
		//Filter filter = statement.getFilter();
		String[] binNames = cmd.binNames;
		//byte[] packedCtx = null;
		//String indexName = null;
		//byte[] packedExp = null;

		/*
		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			// Estimate INDEX_TYPE field.
			if (type != IndexCollectionType.DEFAULT) {
				dataOffset += FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}

			// Estimate INDEX_RANGE field.
			dataOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			filterSize += filter.estimateSize();

			dataOffset += filterSize;
			fieldCount++;

			if (! isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers. Estimate size for selected bin names.
				if (binNames != null && binNames.length > 0) {
					dataOffset += FIELD_HEADER_SIZE;
					binNameSize++;  // num bin names

					for (String binName : binNames) {
						binNameSize += Buffer.estimateSizeUtf8(binName) + 1;
					}
					dataOffset += binNameSize;
					fieldCount++;
				}
			}

			packedCtx = filter.getPackedCtx();
			if (packedCtx != null) {
				dataOffset += FIELD_HEADER_SIZE + packedCtx.length;
				fieldCount++;
			}

			indexName = filter.getIndexName();
			if (indexName != null) {
				dataOffset += FIELD_HEADER_SIZE + Buffer.estimateSizeUtf8(indexName);
				fieldCount++;
			}

			packedExp = filter.getPackedExp();
			if (packedExp != null) {
				dataOffset += FIELD_HEADER_SIZE + packedExp.length;
				fieldCount++;
			}
		}
		*/

		if (cmd.filterExp != null) {
			sizeFieldExpression(cmd.filterExp);
			fieldCount++;
		}

		long maxRecords = 0;
		int partsFullSize = 0;
		int partsPartialDigestSize = 0;
		int partsPartialBValSize = 0;

		if (nodePartitions != null) {
			partsFullSize = nodePartitions.partsFull.size() * 2;
			partsPartialDigestSize = nodePartitions.partsPartial.size() * 20;

			/* TODO Support secondary index query
			if (filter != null) {
				partsPartialBValSize = nodePartitions.partsPartial.size() * 8;
			}
			*/
			maxRecords = nodePartitions.recordMax;
		}

		if (partsFullSize > 0) {
			dataOffset += partsFullSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialDigestSize > 0) {
			dataOffset += partsPartialDigestSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialBValSize > 0) {
			dataOffset += partsPartialBValSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate max records field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (maxRecords > 0) {
			dataOffset += 8 + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
		int operationCount = 0;

		if (binNames != null) {
			// Estimate size for selected bin names (query bin names already handled for old servers).
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			operationCount = binNames.length;
		}

		sizeBuffer();

		int readAttr = Command.INFO1_READ;
		int writeAttr = 0;

		if (cmd.withNoBins) {
			readAttr |= Command.INFO1_NOBINDATA;
		}

		if (cmd.expectedDuration == QueryDuration.SHORT) {
			readAttr |= Command.INFO1_SHORT_QUERY;
		}
		else if (cmd.expectedDuration == QueryDuration.LONG_RELAX_AP) {
			writeAttr |= Command.INFO2_RELAX_AP_LONG_QUERY;
		}

		int infoAttr = Command.INFO3_PARTITION_DONE;

		if (cmd.compress) {
			readAttr |= Command.INFO1_COMPRESS_RESPONSE;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8] = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;

		for (int i = 12; i < 18; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(cmd.readTouchTtlPercent, dataBuffer, 18);
		Buffer.intToBytes(cmd.totalTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;

		if (cmd.namespace != null) {
			writeField(cmd.namespace, FieldType.NAMESPACE);
		}

		if (cmd.set != null) {
			writeField(cmd.set, FieldType.TABLE);
		}

		// Write records per second.
		if (cmd.recordsPerSecond > 0) {
			writeField(cmd.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
		}

		// Write socket idle timeout.
		writeField(cmd.socketTimeout, FieldType.SOCKET_TIMEOUT);

		// Write taskId field
		writeField(taskId, FieldType.QUERY_ID);

		// TODO: Handle secondary index query
		/*
		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			if (type != IndexCollectionType.DEFAULT) {
				writeFieldHeader(1, FieldType.INDEX_TYPE);
				dataBuffer[dataOffset++] = (byte)type.ordinal();
			}

			writeFieldHeader(filterSize, FieldType.INDEX_RANGE);
			dataBuffer[dataOffset++] = (byte)1;
			dataOffset = filter.write(dataBuffer, dataOffset);

			if (!isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers.
				if (binNames != null && binNames.length > 0) {
					writeFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
					dataBuffer[dataOffset++] = (byte)binNames.length;

					for (String binName : binNames) {
						int len = Buffer.stringToUtf8(binName, dataBuffer, dataOffset + 1);
						dataBuffer[dataOffset] = (byte)len;
						dataOffset += len + 1;
					}
				}
			}

			if (packedCtx != null) {
				writeFieldHeader(packedCtx.length, FieldType.INDEX_CONTEXT);
				System.arraycopy(packedCtx, 0, dataBuffer, dataOffset, packedCtx.length);
				dataOffset += packedCtx.length;
			}

			if (indexName != null) {
				writeField(indexName, FieldType.INDEX_NAME);
			}

			if (packedExp != null) {
				writeFieldHeader(packedExp.length, FieldType.INDEX_EXPRESSION);
				System.arraycopy(packedExp, 0, dataBuffer, dataOffset, packedExp.length);
				dataOffset += packedExp.length;
			}
		}
		*/

		if (cmd.filterExp != null) {
			writeFieldExpression(cmd.filterExp);
		}

		if (partsFullSize > 0) {
			writeFieldHeader(partsFullSize, FieldType.PID_ARRAY);

			for (PartitionStatus part : nodePartitions.partsFull) {
				Buffer.shortToLittleBytes(part.id, dataBuffer, dataOffset);
				dataOffset += 2;
			}
		}

		if (partsPartialDigestSize > 0) {
			writeFieldHeader(partsPartialDigestSize, FieldType.DIGEST_ARRAY);

			for (PartitionStatus part : nodePartitions.partsPartial) {
				System.arraycopy(part.digest, 0, dataBuffer, dataOffset, 20);
				dataOffset += 20;
			}
		}

		if (partsPartialBValSize > 0) {
			writeFieldHeader(partsPartialBValSize, FieldType.BVAL_ARRAY);

			for (PartitionStatus part : nodePartitions.partsPartial) {
				Buffer.longToLittleBytes(part.bval, dataBuffer, dataOffset);
				dataOffset += 8;
			}
		}

		if (maxRecords > 0) {
			writeField(maxRecords, FieldType.MAX_RECORDS);
		}

		if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}

		end();
	}

	/*
	public void setQueryBackground(QueryForeground cmd) {
		byte[] functionArgBuffer = null;
		int fieldCount = 0;
		//int filterSize = 0;
		//int binNameSize = 0;

		begin();

		if (cmd.namespace != null) {
			dataOffset += Buffer.estimateSizeUtf8(cmd.namespace) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (cmd.set != null) {
			dataOffset += Buffer.estimateSizeUtf8(cmd.set) + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate recordsPerSecond field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (cmd.recordsPerSecond > 0) {
			dataOffset += 4 + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate socket timeout field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		dataOffset += 4 + Command.FIELD_HEADER_SIZE;
		fieldCount++;

		// Estimate taskId field.
		dataOffset += 8 + Command.FIELD_HEADER_SIZE;
		fieldCount++;

		// TODO: Handle secondary index query
		//Filter filter = statement.getFilter();
		String[] binNames = cmd.binNames;
		//byte[] packedCtx = null;
		//String indexName = null;
		//byte[] packedExp = null;

		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			// Estimate INDEX_TYPE field.
			if (type != IndexCollectionType.DEFAULT) {
				dataOffset += FIELD_HEADER_SIZE + 1;
				fieldCount++;
			}

			// Estimate INDEX_RANGE field.
			dataOffset += FIELD_HEADER_SIZE;
			filterSize++;  // num filters
			filterSize += filter.estimateSize();

			dataOffset += filterSize;
			fieldCount++;

			if (! isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers. Estimate size for selected bin names.
				if (binNames != null && binNames.length > 0) {
					dataOffset += FIELD_HEADER_SIZE;
					binNameSize++;  // num bin names

					for (String binName : binNames) {
						binNameSize += Buffer.estimateSizeUtf8(binName) + 1;
					}
					dataOffset += binNameSize;
					fieldCount++;
				}
			}

			packedCtx = filter.getPackedCtx();
			if (packedCtx != null) {
				dataOffset += FIELD_HEADER_SIZE + packedCtx.length;
				fieldCount++;
			}

			indexName = filter.getIndexName();
			if (indexName != null) {
				dataOffset += FIELD_HEADER_SIZE + Buffer.estimateSizeUtf8(indexName);
				fieldCount++;
			}

			packedExp = filter.getPackedExp();
			if (packedExp != null) {
				dataOffset += FIELD_HEADER_SIZE + packedExp.length;
				fieldCount++;
			}
		}

		// Estimate aggregation/background function size.
		if (cmd.functionName != null) {
			dataOffset += Command.FIELD_HEADER_SIZE + 1;  // udf type
			dataOffset += Buffer.estimateSizeUtf8(cmd.packageName) + Command.FIELD_HEADER_SIZE;
			dataOffset += Buffer.estimateSizeUtf8(cmd.functionName) + Command.FIELD_HEADER_SIZE;

			if (cmd.functionArgs.length > 0) {
				functionArgBuffer = Packer.pack(cmd.functionArgs);
			}
			else {
				functionArgBuffer = new byte[0];
			}
			dataOffset += Command.FIELD_HEADER_SIZE + functionArgBuffer.length;
			fieldCount += 4;
		}

		if (cmd.filterExp != null) {
			sizeFieldExpression(cmd.filterExp);
			fieldCount++;
		}

		long maxRecords = 0;
		int partsFullSize = 0;
		int partsPartialDigestSize = 0;
		int partsPartialBValSize = 0;

		if (cmd.nodePartitions != null) {
			partsFullSize = cmd.nodePartitions.partsFull.size() * 2;
			partsPartialDigestSize = cmd.nodePartitions.partsPartial.size() * 20;

			if (filter != null) {
				partsPartialBValSize = nodePartitions.partsPartial.size() * 8;
			}
			maxRecords = cmd.nodePartitions.recordMax;
		}

		if (partsFullSize > 0) {
			dataOffset += partsFullSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialDigestSize > 0) {
			dataOffset += partsPartialDigestSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		if (partsPartialBValSize > 0) {
			dataOffset += partsPartialBValSize + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Estimate max records field size. This field is used in new servers and not used
		// (but harmless to add) in old servers.
		if (maxRecords > 0) {
			dataOffset += 8 + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}

		// Operations (used in query execute) and bin names (used in scan/query) are mutually exclusive.
		Operation[] operations = cmd.operations;
		int operationCount = 0;

		if (operations != null) {
			// Estimate size for background operations.
			if (! cmd.background) {
				throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Operations not allowed in foreground query");
			}

			for (Operation operation : operations) {
				if (! operation.type.isWrite) {
					throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Read operations not allowed in background query");
				}
				estimateOperationSize(operation);
			}
			operationCount = operations.length;
		}
		else if (binNames != null) {
			// Estimate size for selected bin names (query bin names already handled for old servers).
			for (String binName : binNames) {
				estimateOperationSize(binName);
			}
			operationCount = binNames.length;
		}

		sizeBuffer();

		if (cmd.background) {
			// TODO Handle background query.
			//writeHeaderWrite((WritePolicy)policy, Command.INFO2_WRITE, fieldCount, operationCount);
		}
		else {
			int readAttr = Command.INFO1_READ;
			int writeAttr = 0;

			if (!cmd.includeBinData) {
				readAttr |= Command.INFO1_NOBINDATA;
			}

			if (cmd.expectedDuration == QueryDuration.SHORT) {
				readAttr |= Command.INFO1_SHORT_QUERY;
			}
			else if (cmd.expectedDuration == QueryDuration.LONG_RELAX_AP) {
				writeAttr |= Command.INFO2_RELAX_AP_LONG_QUERY;
			}

			int infoAttr = Command.INFO3_PARTITION_DONE;

			if (cmd.compress) {
				readAttr |= Command.INFO1_COMPRESS_RESPONSE;
			}

			// Write all header data except total size which must be written last.
			dataBuffer[8] = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
			dataBuffer[9] = (byte)readAttr;
			dataBuffer[10] = (byte)writeAttr;
			dataBuffer[11] = (byte)infoAttr;

			for (int i = 12; i < 18; i++) {
				dataBuffer[i] = 0;
			}
			Buffer.intToBytes(cmd.readTouchTtlPercent, dataBuffer, 18);
			Buffer.intToBytes(cmd.totalTimeout, dataBuffer, 22);
			Buffer.shortToBytes(fieldCount, dataBuffer, 26);
			Buffer.shortToBytes(operationCount, dataBuffer, 28);
			dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
		}

		if (cmd.namespace != null) {
			writeField(cmd.namespace, FieldType.NAMESPACE);
		}

		if (cmd.set != null) {
			writeField(cmd.set, FieldType.TABLE);
		}

		// Write records per second.
		if (cmd.recordsPerSecond > 0) {
			writeField(cmd.recordsPerSecond, FieldType.RECORDS_PER_SECOND);
		}

		// Write socket idle timeout.
		writeField(cmd.socketTimeout, FieldType.SOCKET_TIMEOUT);

		// Write taskId field
		writeField(cmd.taskId, FieldType.QUERY_ID);

		// TODO: Handle secondary index query
		if (filter != null) {
			IndexCollectionType type = filter.getCollectionType();

			if (type != IndexCollectionType.DEFAULT) {
				writeFieldHeader(1, FieldType.INDEX_TYPE);
				dataBuffer[dataOffset++] = (byte)type.ordinal();
			}

			writeFieldHeader(filterSize, FieldType.INDEX_RANGE);
			dataBuffer[dataOffset++] = (byte)1;
			dataOffset = filter.write(dataBuffer, dataOffset);

			if (!isNew) {
				// Query bin names are specified as a field (Scan bin names are specified later as operations)
				// in old servers.
				if (binNames != null && binNames.length > 0) {
					writeFieldHeader(binNameSize, FieldType.QUERY_BINLIST);
					dataBuffer[dataOffset++] = (byte)binNames.length;

					for (String binName : binNames) {
						int len = Buffer.stringToUtf8(binName, dataBuffer, dataOffset + 1);
						dataBuffer[dataOffset] = (byte)len;
						dataOffset += len + 1;
					}
				}
			}

			if (packedCtx != null) {
				writeFieldHeader(packedCtx.length, FieldType.INDEX_CONTEXT);
				System.arraycopy(packedCtx, 0, dataBuffer, dataOffset, packedCtx.length);
				dataOffset += packedCtx.length;
			}

			if (indexName != null) {
				writeField(indexName, FieldType.INDEX_NAME);
			}

			if (packedExp != null) {
				writeFieldHeader(packedExp.length, FieldType.INDEX_EXPRESSION);
				System.arraycopy(packedExp, 0, dataBuffer, dataOffset, packedExp.length);
				dataOffset += packedExp.length;
			}
		}

		if (cmd.functionName != null) {
			writeFieldHeader(1, FieldType.UDF_OP);
			dataBuffer[dataOffset++] = cmd.background? (byte)2 : (byte)1;
			writeField(cmd.packageName, FieldType.UDF_PACKAGE_NAME);
			writeField(cmd.functionName, FieldType.UDF_FUNCTION);
			writeField(functionArgBuffer, FieldType.UDF_ARGLIST);
		}

		if (cmd.filterExp != null) {
			writeFieldExpression(cmd.filterExp);
		}

		if (partsFullSize > 0) {
			writeFieldHeader(partsFullSize, FieldType.PID_ARRAY);

			for (PartitionStatus part : cmd.nodePartitions.partsFull) {
				Buffer.shortToLittleBytes(part.id, dataBuffer, dataOffset);
				dataOffset += 2;
			}
		}

		if (partsPartialDigestSize > 0) {
			writeFieldHeader(partsPartialDigestSize, FieldType.DIGEST_ARRAY);

			for (PartitionStatus part : cmd.nodePartitions.partsPartial) {
				System.arraycopy(part.digest, 0, dataBuffer, dataOffset, 20);
				dataOffset += 20;
			}
		}

		if (partsPartialBValSize > 0) {
			writeFieldHeader(partsPartialBValSize, FieldType.BVAL_ARRAY);

			for (PartitionStatus part : cmd.nodePartitions.partsPartial) {
				Buffer.longToLittleBytes(part.bval, dataBuffer, dataOffset);
				dataOffset += 8;
			}
		}

		if (maxRecords > 0) {
			writeField(maxRecords, FieldType.MAX_RECORDS);
		}

		if (operations != null) {
			for (Operation operation : operations) {
				writeOperation(operation);
			}
		}
		else if (binNames != null) {
			for (String binName : binNames) {
				writeOperation(binName, Operation.Type.READ);
			}
		}

		end();
	}
*/
	//--------------------------------------------------
	// Transaction Monitor
	//--------------------------------------------------

	public void setTxnAddKeys(OperateWriteCommand cmd) {
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

	private int estimateKeyAttrSize(Command cmd, Key key, BatchAttr attr, Expression filterExp) {
		int fieldCount = estimateKeySize(cmd, key, attr.hasWrite);

		if (filterExp != null) {
			dataOffset += filterExp.getBytes().length + Command.FIELD_HEADER_SIZE;
			fieldCount++;
		}
		return fieldCount;
	}

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

	private void estimateOperationSize(Operation operation) {
		dataOffset += Buffer.estimateSizeUtf8(operation.binName) + Command.OPERATION_HEADER_SIZE;
		dataOffset += operation.value.estimateSize();
	}

	private void estimateOperationSize(String binName) {
		dataOffset += Buffer.estimateSizeUtf8(binName) + Command.OPERATION_HEADER_SIZE;
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

	/*
	private final void writeHeaderWrite(Command cmd, int writeAttr, int fieldCount, int operationCount) {
		// Set flags.
		int generation = 0;
		int readAttr = 0;
		int infoAttr = 0;
		int txnAttr = 0;

		switch (policy.recordExistsAction) {
		case UPDATE:
			break;
		case UPDATE_ONLY:
			infoAttr |= Command.INFO3_UPDATE_ONLY;
			break;
		case REPLACE:
			infoAttr |= Command.INFO3_CREATE_OR_REPLACE;
			break;
		case REPLACE_ONLY:
			infoAttr |= Command.INFO3_REPLACE_ONLY;
			break;
		case CREATE_ONLY:
			writeAttr |= Command.INFO2_CREATE_ONLY;
			break;
		}

		switch (policy.generationPolicy) {
		case NONE:
			break;
		case EXPECT_GEN_EQUAL:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION;
			break;
		case EXPECT_GEN_GT:
			generation = policy.generation;
			writeAttr |= Command.INFO2_GENERATION_GT;
			break;
		}

		if (policy.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}

		if (policy.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (policy.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (policy.xdr) {
			readAttr |= Command.INFO1_XDR;
		}

		// Write all header data except total size which must be written last.
		dataBuffer[8]  = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;
		dataBuffer[12] = (byte)txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(generation, dataBuffer, 14);
		Buffer.intToBytes(policy.expiration, dataBuffer, 18);
		Buffer.intToBytes(cmd.serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
	}
	*/

	private void writeHeaderOperate(OperateWriteCommand cmd, int fieldCount) {
		// Set flags.
		int gen = 0;
		// Operate() is currently defined as only write operations.
		//int ttl = cmd.hasWrite ? cmd.ttl : cmd.readTouchTtlPercent;
		int ttl = cmd.ttl;
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

		// Operate() is currently defined as only write operations.
		/*
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
		*/

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

	private void writeHeaderRead(
		ReadCommand cmd,
		int timeout,
		int readAttr,
		int writeAttr,
		int infoAttr,
		int fieldCount,
		int operationCount
	) {
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
		dataBuffer[8] = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9] = (byte)readAttr;
		dataBuffer[10] = (byte)writeAttr;
		dataBuffer[11] = (byte)infoAttr;

		for (int i = 12; i < 18; i++) {
			dataBuffer[i] = 0;
		}
		Buffer.intToBytes(cmd.readTouchTtlPercent, dataBuffer, 18);
		Buffer.intToBytes(timeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;
	}

	private void writeKeyAttr(
		Command cmd, Key key, BatchAttr attr, Expression filterExp, int fieldCount,
		int operationCount
	) {
		// Write all header data except total size which must be written last.
		dataBuffer[8]  = Command.MSG_REMAINING_HEADER_SIZE; // Message header length.
		dataBuffer[9]  = (byte)attr.readAttr;
		dataBuffer[10] = (byte)attr.writeAttr;
		dataBuffer[11] = (byte)attr.infoAttr;
		dataBuffer[12] = (byte)attr.txnAttr;
		dataBuffer[13] = 0; // clear the result code
		Buffer.intToBytes(attr.generation, dataBuffer, 14);
		Buffer.intToBytes(attr.expiration, dataBuffer, 18);
		Buffer.intToBytes(cmd.serverTimeout, dataBuffer, 22);
		Buffer.shortToBytes(fieldCount, dataBuffer, 26);
		Buffer.shortToBytes(operationCount, dataBuffer, 28);
		dataOffset = Command.MSG_TOTAL_HEADER_SIZE;

		writeKey(cmd, key, attr.hasWrite);

		if (filterExp != null) {
			writeFieldExpression(filterExp);
		}
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

	private void writeField(long val, int type) {
		writeFieldHeader(8, type);
		Buffer.longToBytes(val, dataBuffer, dataOffset);
		dataOffset += 8;
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

	private void writeField(int val, int type) {
		writeFieldHeader(4, type);
		Buffer.intToBytes(val, dataBuffer, dataOffset);
		dataOffset += 4;
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

	private void writeOperation(String name, Operation.Type operation) {
		int nameLength = Buffer.stringToUtf8(name, dataBuffer, dataOffset + Command.OPERATION_HEADER_SIZE);

		Buffer.intToBytes(nameLength + 4, dataBuffer, dataOffset);
		dataOffset += 4;
		dataBuffer[dataOffset++] = (byte) operation.protocolType;
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) 0;
		dataBuffer[dataOffset++] = (byte) nameLength;
		dataOffset += nameLength;
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

	private void compress(Command cmd) {
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
