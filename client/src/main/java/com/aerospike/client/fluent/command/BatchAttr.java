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

import java.util.List;

import com.aerospike.client.fluent.Operation;
import com.aerospike.client.fluent.exp.Expression;
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

	/*
	public BatchAttr(BatchReadCommand cmd, int rattr) {
		setRead(cmd);
		this.readAttr |= rattr;
	}

	public BatchAttr(BatchReadCommand cmd, int rattr, List<Operation> ops) {
		setRead(cmd);
		this.readAttr = rattr;

		if (ops != null) {
			adjustRead(ops);
		}
	}

	public BatchAttr(BatchReadCommand cmd, BatchWritePolicy wp, List<Operation> ops) {
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

	public void setReadSingle(BatchReadCommand cmd, BatchRead br) {
		Expression e = (br.filterExp != null)? br.filterExp : cmd.filterExp;
		setRead(cmd, e);
	}

	public void setReadEntry(BatchReadCommand cmd, BatchRead br) {
		setRead(cmd, br.filterExp);
	}

	private void setRead(BatchReadCommand cmd, Expression e) {
		filterExp = e;
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

	public void adjustRead(List<Operation> ops) {
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

	public void setWriteSingle(BatchWriteCommand cmd, BatchWrite bw) {
		Expression e = (bw.filterExp != null)? bw.filterExp : cmd.filterExp;
		setWrite(cmd, bw, e);
	}

	public void setWriteEntry(BatchWriteCommand cmd, BatchWrite bw) {
		setWrite(cmd, bw, bw.filterExp);
	}

	private void setWrite(BatchWriteCommand cmd, BatchWrite bw, Expression e) {
		filterExp = e;
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

		case REPLACE_IF_EXISTS:
			infoAttr |= Command.INFO3_REPLACE_ONLY;
			break;
		}

		if (cmd.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (cmd.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void adjustWrite(List<Operation> ops) {
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

	public void setUDFSingle(BatchWriteCommand cmd, BatchUDF bu) {
		Expression e = (bu.filterExp != null)? bu.filterExp : cmd.filterExp;
		setUDF(cmd, bu, e);
	}

	public void setUDFEntry(BatchWriteCommand cmd, BatchUDF bu) {
		setUDF(cmd, bu, bu.filterExp);
	}

	private void setUDF(BatchWriteCommand cmd, BatchUDF bu, Expression e) {
		filterExp = e;
		readAttr = 0;
		writeAttr = Command.INFO2_WRITE;
		infoAttr = 0;
		txnAttr = 0;
		expiration = bu.ttl;
		generation = 0;
		hasWrite = true;
		sendKey = cmd.sendKey;

		if (cmd.durableDelete) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (cmd.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setDeleteSingle(BatchWriteCommand cmd, BatchDelete bd) {
		Expression e = (bd.filterExp != null)? bd.filterExp : cmd.filterExp;
		setDelete(cmd, bd, e);
	}

	public void setDeleteEntry(BatchWriteCommand cmd, BatchDelete bd) {
		setDelete(cmd, bd, bd.filterExp);
	}

	private void setDelete(BatchWriteCommand cmd, BatchDelete bd, Expression e) {
		filterExp = e;
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

		if (cmd.commitLevel == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setOpSize(List<Operation> ops) {
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
