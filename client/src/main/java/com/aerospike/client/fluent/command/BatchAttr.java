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

import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.exp.Expression;
import com.aerospike.client.fluent.policy.BatchReadPolicy;
import com.aerospike.client.fluent.policy.BatchUDFPolicy;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.ReadModeAP;

public final class BatchAttr {
	public Expression filterExp;
	public int readAttr;
	public int writeAttr;
	public int infoAttr;
	public int txnAttr;
	public int expiration;
	public int opSize;
	public short generation;
	public boolean hasWrite;
	public boolean sendKey;

	public BatchAttr() {
	}

	public BatchAttr(BatchReadCommand cmd, int rattr) {
		setRead(cmd);
		this.readAttr |= rattr;
	}

	public BatchAttr(BatchReadCommand cmd, int rattr, Operation[] ops) {
		setRead(cmd);
		this.readAttr = rattr;

		if (ops != null) {
			adjustRead(ops);
		}
	}

	/*
	public BatchAttr(BatchReadCommand cmd, BatchWritePolicy wp, Operation[] ops) {
		boolean readAllBins = false;
		boolean readHeader = false;
		boolean hasRead = false;
		boolean hasWriteOp = false;

		for (Operation op : ops) {
			if (op.type.isWrite) {
				hasWriteOp = true;
			}
			else {
				hasRead = true;

				if (op.type == Operation.Type.READ) {
					if (op.binName == null) {
						readAllBins = true;
					}
				}
				else if (op.type == Operation.Type.READ_HEADER) {
					readHeader = true;
				}
			}
		}

		if (hasWriteOp) {
			setWrite(wp);

			if (hasRead) {
				readAttr |= Command.INFO1_READ;

				if (readAllBins) {
					readAttr |= Command.INFO1_GET_ALL;
					// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
					writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
				}
				else if (readHeader) {
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}
		else {
			setRead(cmd);

			if (readAllBins) {
				readAttr |= Command.INFO1_GET_ALL;
			}
			else if (readHeader) {
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}
	}
	*/

	public void setRead(BatchReadCommand cmd) {
		filterExp = null;
		readAttr = Command.INFO1_READ;

		if (cmd.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeAttr = 0;

		switch (cmd.readModeSC) {
		default:
		case SESSION:
			infoAttr = 0;
			break;
		case LINEARIZE:
			infoAttr = Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr = Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}
		txnAttr = 0;
		expiration = cmd.readTouchTtlPercent;
		generation = 0;
		hasWrite = false;
		sendKey = false;
	}

	public void setRead(BatchReadPolicy rp) {
		filterExp = rp.filterExp;
		readAttr = Command.INFO1_READ;

		if (rp.readModeAP == ReadModeAP.ALL) {
			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
		}

		writeAttr = 0;

		switch (rp.readModeSC) {
		default:
		case SESSION:
			infoAttr = 0;
			break;
		case LINEARIZE:
			infoAttr = Command.INFO3_SC_READ_TYPE;
			break;
		case ALLOW_REPLICA:
			infoAttr = Command.INFO3_SC_READ_RELAX;
			break;
		case ALLOW_UNAVAILABLE:
			infoAttr = Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX;
			break;
		}
		txnAttr = 0;
		expiration = rp.readTouchTtlPercent;
		generation = 0;
		hasWrite = false;
		sendKey = false;
	}

	public void adjustRead(Operation[] ops) {
		for (Operation op : ops) {
			if (op.type == Operation.Type.READ) {
				if (op.binName == null) {
					readAttr |= Command.INFO1_GET_ALL;
				}
			}
			else if (op.type == Operation.Type.READ_HEADER) {
				readAttr |= Command.INFO1_NOBINDATA;
			}
		}
	}

	public void adjustRead(boolean readAllBins) {
		if (readAllBins) {
			readAttr |= Command.INFO1_GET_ALL;
		}
		else {
			readAttr |= Command.INFO1_NOBINDATA;
		}
	}

	public void setWrite(BatchWriteCommand cmd, BatchWrite bw) {
		filterExp = cmd.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS;
		infoAttr = 0;
		txnAttr = 0;
		expiration = bw.ttl;
		hasWrite = true;
		sendKey = cmd.sendKey;

		if (bw.generation > 0) {
			generation = (short)bw.generation;
			writeAttr |= Command.INFO2_GENERATION;
		}

		switch (bw.opType) {
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

		if (cmd.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}
	}

	public void adjustWrite(Operation[] ops) {
		for (Operation op : ops) {
			if (! op.type.isWrite) {
				readAttr |= Command.INFO1_READ;

				if (op.type == Operation.Type.READ) {
					if (op.binName == null) {
						readAttr |= Command.INFO1_GET_ALL;
						// When GET_ALL is specified, RESPOND_ALL_OPS must be disabled.
						writeAttr &= ~Command.INFO2_RESPOND_ALL_OPS;
					}
				}
				else if (op.type == Operation.Type.READ_HEADER) {
					readAttr |= Command.INFO1_NOBINDATA;
				}
			}
		}
	}

	public void setUDF(BatchUDFPolicy up) {
		filterExp = up.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE;
		infoAttr = 0;
		txnAttr = 0;
		expiration = up.expiration;
		generation = 0;
		hasWrite = true;
		sendKey = up.sendKey;

		if (up.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (up.onLockingOnly) {
			txnAttr |= Command.INFO4_TXN_ON_LOCKING_ONLY;
		}

		if (up.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setDelete(BatchWriteCommand cmd, BatchDelete bd) {
		filterExp = cmd.filterExp;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DELETE;
		infoAttr = 0;
		txnAttr = 0;
		expiration = 0;
		hasWrite = true;
		sendKey = cmd.sendKey;

		if (bd.generation > 0) {
			generation = (short)bd.generation;
			writeAttr |= Command.INFO2_GENERATION;
		}

		if (cmd.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}
	}

	public void setOpSize(Operation[] ops) {
		int dataOffset = 0;

		for (Operation op : ops) {
			dataOffset += Buffer.estimateSizeUtf8(op.binName) + Command.OPERATION_HEADER_SIZE;
			dataOffset += op.value.estimateSize();
		}
		opSize = dataOffset;
	}

	public void setTxn(int attr) {
		filterExp = null;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DURABLE_DELETE;
		infoAttr = 0;
		txnAttr = attr;
		expiration = 0;
		generation = 0;
		hasWrite = true;
		sendKey = false;
	}
}
