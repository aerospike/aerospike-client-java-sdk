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
package com.aerospike.client.sdk.task;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.util.Version;

/**
 * Task used to poll for long running create index completion.
 */
public final class IndexTask extends Task {
    private final String namespace;
    private final String indexName;
    private final boolean isCreate;
    private String statusCommand;
    private String existsCommand;

    /**
     * Initialize task with fields needed to query server nodes.
     */
    public IndexTask(Cluster cluster, String namespace, String indexName, boolean isCreate, int timeout) {
        super(cluster, timeout);
        this.namespace = namespace;
        this.indexName = indexName;
        this.isCreate = isCreate;
    }

    /**
     * Query all nodes for task completion status.
     */
    @Override
    public int queryStatus() {
        // All nodes must respond with load_pct of 100 to be considered done.
        Node[] nodes = cluster.validateNodes();

        for (Node node : nodes) {
            Version currentServerVersion = node.getVersion();
            if (isCreate) {
                // Check index status.
                if (statusCommand == null) {
                    statusCommand = IndexTask.buildStatusCommand(namespace, indexName, currentServerVersion);
                }

                String response = Info.request(node, statusCommand, timeout);
                int status = parseStatusResponse(statusCommand, response, isCreate);

                if (status != Task.COMPLETE) {
                    return status;
                }
            }
            else {
                // Check if index exists.
                if (existsCommand == null) {
                    existsCommand = buildExistsCommand(namespace, indexName, currentServerVersion);
                }

                String response = Info.request(node, existsCommand, timeout);
                int status = parseExistsResponse(existsCommand, response);

                if (status != Task.COMPLETE) {
                    return status;
                }
            }
        }
        return Task.COMPLETE;
    }

    public static String buildStatusCommand(String namespace, String indexName, Version serverVersion) {
        return serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ?
            "sindex-stat:namespace=" + namespace + ";indexname=" + indexName :
            "sindex/" + namespace + "/" + indexName;
    }

    public static String buildExistsCommand(String namespace, String indexName, Version currentServerVersion) {
        return currentServerVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ?
            "sindex-exists:namespace=" + namespace + ";indexname=" + indexName :
            "sindex-exists:ns=" + namespace + ";indexname=" + indexName;
    }

    public static int parseStatusResponse(String command, String response, boolean isCreate) { if (isCreate) {
            // Check if index has been created.
            String find = "load_pct=";
            int index = response.indexOf(find);

            if (index < 0) {
                Info.Error error = new Info.Error(response);

                if (error.code == ResultCode.INDEX_NOTFOUND || error.code == ResultCode.INDEX_NOTREADABLE) {
                    return Task.NOT_FOUND;
                }
                else {
                    // Throw exception immediately.
                    throw AerospikeException.resultCodeToException(error.code, command + " failed: " + error.message);
                }
            }

            int begin = index + find.length();
            int end = response.indexOf(';', begin);
            String str = response.substring(begin, end);
            int pct = Integer.parseInt(str);

            if (pct != 100) {
                return Task.IN_PROGRESS;
            }
        }
        else {
            // Check if index has been dropped.
            Info.Error error = new Info.Error(response);

            if (error.code != ResultCode.INDEX_NOTFOUND) {
                // Index still exists.
                return Task.IN_PROGRESS;
            }
        }
        return Task.COMPLETE;
    }

    public static int parseExistsResponse(String command, String response) {
        if (response.equals("false")) {
            return Task.COMPLETE;
        }

        if (response.equals("true")) {
            return Task.IN_PROGRESS;
        }

        throw new AerospikeException(command + " failed: " + response);
    }
}
