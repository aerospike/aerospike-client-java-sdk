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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.util.Util;

/**
 * Default metrics listener. This implementation writes periodic metrics snapshots to a file which
 * will later be read and forwarded to OpenTelemetry by a separate offline application.
 */
public final class MetricsWriter implements IMetricsExporter {
    private static final Logger log = LoggerFactory.getLogger(Loggers.METRICS);
    private static final DateTimeFormatter FilenameFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private static final DateTimeFormatter TimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	private static final int MinFileSize = 1000000;

	private String dir;
	private StringBuilder sb;
	private FileWriter writer;
	private long size;
	private long maxSize;
    private TimeUnit latencyUnit;
	private int latencyColumns;
	private int latencyShift;
	private boolean enabled;

	/**
	 * Metrics writer constructor. Open timestamped metrics file in append mode and write header
	 * indicating what metrics will be stored.
	 */
	public MetricsWriter(MetricsSettings settings) {
        if (settings.getReportSizeLimit() != 0 && settings.getReportSizeLimit() < MinFileSize) {
            throw new AerospikeException("MetricsSettings.reportSizeLimit " + settings.getReportSizeLimit() +
                " must be at least " + MinFileSize);
        }

        this.dir = settings.getReportDir();
        this.sb = new StringBuilder(8192);
        this.maxSize = settings.getReportSizeLimit();

        MetricsExtended extended = settings.getExtended();
        MetricsOperational operational = extended.getOperational();
        this.latencyUnit = operational.getLatencyUnit();
        this.latencyColumns = operational.getLatencyColumns();
        this.latencyShift = operational.getLatencyShift();

        try {
            Files.createDirectories(Paths.get(dir));
            open();
        }
        catch (IOException ioe) {
            throw new AerospikeException(ioe);
        }

        enabled = true;
	}

	/**
	 * Write cluster metrics snapshot to file.
	 */
	@Override
	public void export(MetricsSnapshot snapshot) {
		if (enabled) {
			writeCluster(snapshot);
		}
	}

	/**
	 * Write final cluster metrics snapshot to file and then close the file.
	 */
	public void onDisable() {
		if (enabled) {
			try {
				enabled = false;
				writer.close();
			}
			catch (Throwable e) {
			    if (log.isErrorEnabled()) {
                    log.error("Failed to close metrics writer: " + Util.getErrorMessage(e));
                }
			}
		}
	}

	private void open() throws IOException {
		LocalDateTime now = LocalDateTime.now();
		String path = dir + File.separator + "metrics-" + now.format(FilenameFormat) + ".log";
		writer = new FileWriter(path, false);
		size = 0;

		// Must use separate StringBuilder instance to avoid conflicting with metrics detail write.
		sb.setLength(0);
		sb.append(now.format(TimestampFormat));
		sb.append(" header(5)");
		sb.append(" cluster[cluster_name,client_type,client_version,app_id,labels[],cpu,mem,recover_queue_size,nodes_invalid,command_count,blocking_count,deferred_count,background_count,tran_count,command_retries,nodes[]]");
		sb.append(" labels[name,value]");
		sb.append(" nodes[name,address,port,conns_in_use,conns_in_pool,conns_opened,conns_closed,namespaces[]]");
		sb.append(" namespaces[name,errors,timeouts,key_busy,bytes_in,bytes_out,latency[]]");
		sb.append(" latency(");
        sb.append(latencyUnit);
        sb.append(',');
		sb.append(latencyColumns);
		sb.append(',');
		sb.append(latencyShift);
		sb.append(')');
		sb.append("[type[l1,l2,l3...]]");
		writeLine();
	}

	private void writeCluster(MetricsSnapshot ms) {
		sb.setLength(0);
		sb.append(ms.timestamp.format(TimestampFormat));
		sb.append(" cluster[");
		sb.append(ms.clusterName);
		sb.append(',');
		sb.append("java-sdk");
		sb.append(',');
		sb.append(ms.clientVersion);
		sb.append(',');
        sb.append(ms.appId);
		sb.append(',');

		Map<String,String> map = ms.labels;

		sb.append("[");
		for (String key : map.keySet()) {
			sb.append("[").append(key).append(",").append(map.get(key)).append("],");
		}

		if (map.size() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
		sb.append("]");

		sb.append(',');
		sb.append(ms.cpuPercent);
		sb.append(',');
		sb.append(ms.memoryBytes);
		sb.append(',');
		sb.append(ms.recoverQueueSize);
		sb.append(',');
		sb.append(ms.invalidNodeCount);
        sb.append(',');
        sb.append(ms.commandCount);

        UsageMetricsSnapshot usage = ms.usage;

        sb.append(',');
        sb.append(usage.blocking);
        sb.append(',');
        sb.append(usage.deferred);
        sb.append(',');
        sb.append(usage.background);
        sb.append(',');
        sb.append(usage.transactions);

        sb.append(',');
		sb.append(ms.commandRetries);
		sb.append(",[");

		int count = 0;

        for (NodeMetricsSnapshot node : ms.nodes) {
            if (count > 0) {
                sb.append(',');
            }
            writeNode(node);
            count++;
        }

        for (NodeMetricsSnapshot node : ms.nodesDeparted) {
            if (count > 0) {
                sb.append(',');
            }
            writeNode(node);
            count++;
        }

		sb.append("]");
		writeLine();
	}

	private void writeNode(NodeMetricsSnapshot node) {
 		sb.append('[');
		sb.append(node.nodeName);
		sb.append(',');
		sb.append(node.nodeAddress);
		sb.append(',');
		sb.append(node.nodePort);

		ConnectionMetricsSnapshot cms = node.connections;

        sb.append(',');
        sb.append(cms.inUse);
        sb.append(',');
        sb.append(cms.inPool);
        sb.append(',');
        sb.append(cms.opened);
        sb.append(',');
        sb.append(cms.closed);
		sb.append(",[");

		NamespaceMetricsSnapshot[] nms = node.namespaces;

        for (int i = 0; i < nms.length; i++) {
            if (i > 0) {
                sb.append(",[");
            }

            NamespaceMetricsSnapshot ns = nms[i];

            sb.append(ns.namespace);
            sb.append(',');
            sb.append(ns.errors);
            sb.append(',');
            sb.append(ns.clientTimeouts);
            sb.append(',');
            sb.append(ns.keyBusy);
            sb.append(',');
            sb.append(ns.bytesIn);
            sb.append(',');
            sb.append(ns.bytesOut);
            sb.append(",[");

            LatencySnapshot[] latencies = ns.latencies;

            for (int j = 0; j < latencies.length; j++) {
                if (j > 0) {
                    sb.append(',');
                }

                LatencySnapshot ls = latencies[j];

                sb.append(ls.type.getLabel());
                sb.append('[');

                long[] buckets = ls.bucketCounts;

                for (int k = 0; k < buckets.length; k++) {
                    if (k > 0) {
                        sb.append(',');
                    }
                    sb.append(buckets[k]);
                }
                sb.append(']');
            }
            sb.append("]]");
        }
        sb.append("]]");
	}

	private void writeLine() {
		try {
			sb.append(System.lineSeparator());
			writer.write(sb.toString());
			size += sb.length();
			writer.flush();

			if (maxSize > 0 && size >= maxSize) {
				writer.close();

				// This call is recursive since open() calls writeLine() to write the header.
				open();
			}
		}
		catch (IOException ioe) {
			enabled = false;

			try {
				writer.close();
			}
			catch (Throwable t) {
			}

			throw new AerospikeException(ioe);
		}
	}
}
