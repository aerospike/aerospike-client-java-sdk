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

import com.aerospike.client.fluent.OpType;
import com.aerospike.client.fluent.policy.CommitLevel;
import com.aerospike.client.fluent.policy.ReadModeAP;
import com.aerospike.client.fluent.policy.Replica;
import com.aerospike.client.fluent.policy.Settings;

public final class BatchAttr {
	public Replica replica;
	public byte readAttr;
	public byte writeAttr;
	public byte infoAttr;
	public byte txnAttr;
	public boolean hasWrite;
    public boolean linearize;

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

	public void setRead(Settings settings, boolean scMode) {
		readAttr = Command.INFO1_READ;
		writeAttr = 0;
		txnAttr = 0;
		hasWrite = false;

		if (scMode) {
            switch (settings.getReadModeSC()) {
            case SESSION:
            	replica = Replica.MASTER;
        		infoAttr = 0;
            	linearize = false;
                break;

            case LINEARIZE:
                if (settings.getReplicaOrder() == Replica.PREFER_RACK) {
                    replica = Replica.SEQUENCE;
                }
                else {
                	replica = settings.getReplicaOrder();
                }
    			infoAttr = Command.INFO3_SC_READ_TYPE;
                linearize = true;
                break;

    		case ALLOW_REPLICA:
            	replica = settings.getReplicaOrder();
            	infoAttr = (byte)Command.INFO3_SC_READ_RELAX;
            	linearize = false;
    			break;

    		case ALLOW_UNAVAILABLE:
            	replica = settings.getReplicaOrder();
    			infoAttr = (byte)(Command.INFO3_SC_READ_TYPE | Command.INFO3_SC_READ_RELAX);
            	linearize = false;
    			break;
            }
        }
        else {
    		if (settings.getReadModeAP() == ReadModeAP.ALL) {
    			readAttr |= Command.INFO1_READ_MODE_AP_ALL;
    		}
    		infoAttr = 0;
            replica = settings.getReplicaOrder();
            linearize = false;
        }
	}

	public void setWrite(Settings settings, OpType type) {
		readAttr = 0;
		writeAttr = (byte)(Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS);
		infoAttr = 0;
		txnAttr = 0;
		hasWrite = true;

		switch (type) {
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

		default:
			break;
		}

		if (settings.getUseDurableDelete()) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (settings.getCommitLevel() == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	/*
	public void setUDFSingle(BatchWriteCommand cmd, BatchUDF bu) {
		Expression e = (bu.where != null)? bu.where : cmd.filterExp;
		setUDF(cmd, bu, e);
	}

	public void setUDFEntry(BatchWriteCommand cmd, BatchUDF bu) {
		setUDF(cmd, bu, bu.where);
	}

	private void setUDF(BatchWriteCommand cmd, BatchUDF bu, Expression e) {
		where = e;
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
*/

	public void setDelete(Settings settings) {
		readAttr = 0;
		writeAttr = (byte)(Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DELETE);
		infoAttr = 0;
		txnAttr = 0;
		hasWrite = true;

		if (settings.getUseDurableDelete()) {
			writeAttr |= Command.INFO2_DURABLE_DELETE;
		}

		if (settings.getCommitLevel() == CommitLevel.COMMIT_MASTER) {
			infoAttr |= Command.INFO3_COMMIT_MASTER;
		}
	}

	public void setTxn(int attr) {
		readAttr = 0;
		writeAttr = (byte)(Command.INFO2_WRITE | Command.INFO2_RESPOND_ALL_OPS | Command.INFO2_DURABLE_DELETE);
		infoAttr = 0;
		txnAttr = (byte)attr;
		hasWrite = true;
	}
}
