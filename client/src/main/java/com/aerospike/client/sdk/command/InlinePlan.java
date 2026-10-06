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
package com.aerospike.client.sdk.command;

import java.util.Objects;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.PartitionTracker.NodePartitions;
import com.aerospike.client.sdk.query.plan.QuerySelection;
import com.aerospike.client.sdk.query.plan.QueryPlanStats;
import com.aerospike.client.sdk.query.plan.QueryWhereWire;

/**
 * Plan state of one AUTO_PLAN query (approach A2) - lives as long as its {@link QueryCommand},
 * across every chunk and retry round of that query.
 *
 * <p>The first command to each node carries {@code AUTO_PLAN} and no pin; each node plans
 * locally and answers with a plan header. Every later command - a partition retry or the next
 * chunk - pins the plan the nodes reported, because resume cursors are only valid on the access
 * path that produced them. A2 assumes one global plan: if nodes reported different plans, a query
 * that needs another command fails rather than resuming partitions on the wrong path.</p>
 */
public final class InlinePlan {

    public record Choice(QuerySelection selection, String indexName) {
        public boolean isSecondaryIndex() {
            return selection == QuerySelection.SECONDARY_INDEX;
        }
    }

    /** First command: AUTO_PLAN plus the caller's policy flags. */
    final byte[] autoWhere;
    /** Pinned sindex: AUTO_PLAN with INDEX_NAME, range re-derived by the server. */
    final byte[] pinWhere;
    /** Pinned primary-index scan: plain execute WHERE, exactly as two-phase sends it. */
    final byte[] scanWhere;

    /**
     * A4: remember the plan per partition instead of requiring one global plan. Each partition is
     * resumed on the path its node reported, so nodes that disagree (an index still building on
     * one node, differing local stats) no longer stop the query.
     */
    final boolean perPartition;

    private Choice agreed;
    private boolean seen;
    private boolean disagreed;

    public InlinePlan(String ael, int policyFlags) {
        this(ael, policyFlags, false);
    }

    public InlinePlan(String ael, int policyFlags, boolean perPartition) {
        this.autoWhere = QueryWhereWire.encode(QueryWhereWire.FLAG_AUTO_PLAN | policyFlags, ael);
        this.pinWhere = QueryWhereWire.encode(QueryWhereWire.FLAG_AUTO_PLAN, ael);
        this.scanWhere = QueryWhereWire.forExecute(ael);
        this.perPartition = perPartition;
    }

    /**
     * Header from the node serving {@code np}. With per-partition plans, every partition in that
     * command that has no plan yet takes this one - records (and so cursors) only follow the
     * header, so no partition can hold a cursor from an unrecorded path.
     */
    public void onHeader(NodePartitions np, Choice choice) {
        if (perPartition && np != null) {
            synchronized (this) {
                for (PartitionStatus ps : np.partsFull) {
                    if (ps.plan == null) {
                        ps.plan = choice;
                    }
                }

                for (PartitionStatus ps : np.partsPartial) {
                    if (ps.plan == null) {
                        ps.plan = choice;
                    }
                }
            }
        }

        onHeader(choice);
    }

    /**
     * Plan to pin on the command for {@code np}, or {@code null} to plan inline.
     */
    public Choice pinnedFor(NodePartitions np) {
        if (perPartition) {
            return np != null ? np.plan : null;
        }
        return pinned();
    }

    public synchronized void onHeader(Choice choice) {
        QueryPlanStats.planHeaders.increment();

        if (! seen) {
            seen = true;
            agreed = choice;
            return;
        }

        if (! disagreed && ! Objects.equals(agreed, choice)) {
            disagreed = true;
            QueryPlanStats.planDisagreements.increment();
        }
    }

    /**
     * Plan to pin on the next command, or {@code null} if no node has answered yet.
     */
    public synchronized Choice pinned() {
        if (! seen) {
            return null;
        }

        if (disagreed) {
            throw new AerospikeException(ResultCode.PARAMETER_ERROR,
                "Nodes chose different query plans; continuing this query would resume partitions on "
                    + "a different access path (single global plan only)");
        }
        return agreed;
    }

    public synchronized boolean isDisagreed() {
        return disagreed;
    }
}
