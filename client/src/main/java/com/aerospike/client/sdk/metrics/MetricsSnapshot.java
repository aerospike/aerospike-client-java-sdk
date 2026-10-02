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
package com.aerospike.client.sdk.metrics;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.MetricsExtended;
import com.aerospike.client.sdk.MetricsOperational;
import com.aerospike.client.sdk.MetricsSettings;
import com.aerospike.client.sdk.MetricsUsage;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.command.Buffer;
import com.aerospike.client.sdk.util.Util;

/**
 * Metrics snapshot.
 */
public final class MetricsSnapshot {
	public final LocalDateTime timestamp;
    public final String clusterName;
    public final String clientType;
    public final String clientVersion;
    public final String appId;
    public final Map<String,String> labels;
    public final int totalNodes;
    public final int recoverQueueSize;
    public final int invalidNodeCount;
    public final long commandCount;
    public final long commandRetries;
    public final double cpuPercent;
    public final long memoryBytes;
    public final boolean metricsEnabled;
    public final boolean operationalMetricsEnabled;
    public final boolean usageMetricsEnabled;
    public final NodeMetricsSnapshot[] nodes;
    public final NodeMetricsSnapshot[] nodesDeparted;
    public final UsageMetricsSnapshot usage;
    public final TimeUnit latencyUnit;
    public final int latencyColumns;
    public final int latencyShift;

    /**
     * Metrics snapshot constructor.
     */
    public MetricsSnapshot(Cluster cluster, MetricsSettings settings) {
        this.timestamp = LocalDateTime.now();
        this.clusterName = (cluster.getClusterName() != null)? cluster.getClusterName() : "";
        this.clientType = "java-sdk";
        this.clientVersion = cluster.getVersion().toString();

        ClusterDefinition def = cluster.getClusterDefinition();

        if (def.getAppId() != null) {
            this.appId = def.getAppId();
        }
        else {
            byte[] userBytes = def.getUserName();

            if (userBytes != null && userBytes.length > 0) {
                this.appId = Buffer.utf8ToString(userBytes, 0, userBytes.length);
            }
            else {
                this.appId = "";
            }
        }

        this.labels = (settings.getLabels() != null)? settings.getLabels() : new HashMap<>();
        this.recoverQueueSize = cluster.getRecoverQueueSize();
        this.invalidNodeCount = cluster.getInvalidNodeCount();
        this.commandCount = cluster.getBlockingCount() + cluster.getDeferredCount() + cluster.getBackgroundCount();
        this.commandRetries = cluster.getRetryCount();
        this.cpuPercent = Util.getProcessCpuLoad();
        this.memoryBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        this.metricsEnabled = settings.getEnabled();

        MetricsExtended me = settings.getExtended();
        Node[] nodes = cluster.getNodes();

        this.totalNodes = nodes.length;
        this.nodes = new NodeMetricsSnapshot[totalNodes];

        for (int i = 0; i < totalNodes; i++) {
            NodeMetricsSnapshot nms = new NodeMetricsSnapshot(nodes[i]);
            this.nodes[i] = nms;
        }

        List<NodeMetricsSnapshot> list = cluster.getNodesDeparted();
        this.nodesDeparted = list.toArray(new NodeMetricsSnapshot[0]);

        MetricsOperational mo = me.getOperational();

        this.operationalMetricsEnabled = mo.getEnabled();
        this.latencyUnit = mo.getLatencyUnit();
        this.latencyColumns = mo.getLatencyColumns();
        this.latencyShift = mo.getLatencyShift();

        MetricsUsage mu = me.getUsage();

        this.usageMetricsEnabled = mu.getEnabled();
        this.usage = new UsageMetricsSnapshot(cluster);
    }

    @Override
    public String toString() {
        return "MetricsSnapshot{"
            + "timestamp=" + timestamp
            + ", clusterName=" + clusterName
            + ", clientType=" + clientType
            + ", clientVersion=" + clientVersion
            + ", appId=" + appId
            + ", labels=" + labels
            + ", totalNodes=" + totalNodes
            + ", recoverQueueSize=" + recoverQueueSize
            + ", invalidNodeCount=" + invalidNodeCount
            + ", commandCount=" + commandCount
            + ", commandRetries=" + commandRetries
            + ", cpuPercent=" + cpuPercent
            + ", memoryBytes=" + memoryBytes
            + ", metricsEnabled=" + metricsEnabled
            + ", operationalMetricsEnabled=" + operationalMetricsEnabled
            + ", usageMetricsEnabled=" + usageMetricsEnabled
            + ", nodes=" + Arrays.toString(nodes)
            + ", nodesDeparted=" + Arrays.toString(nodesDeparted)
            + ", usage=" + usage
            + ", latencyUnit=" + latencyUnit
            + ", latencyColumns=" + latencyColumns
            + ", latencyShift=" + latencyShift
            + '}';
    }
}
