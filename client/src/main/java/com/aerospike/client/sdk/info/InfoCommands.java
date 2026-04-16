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
package com.aerospike.client.sdk.info;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.info.classes.NamespaceDetail;
import com.aerospike.client.sdk.info.classes.SetDetail;
import com.aerospike.client.sdk.info.classes.Sindex;
import com.aerospike.client.sdk.info.classes.SindexDetail;

/**
 * Provides high-level methods to execute common Aerospike info commands.
 *
 * <p>This class encapsulates the most commonly used Aerospike info commands and provides
 * a convenient API for retrieving cluster information. It supports two types of operations:</p>
 * <ul>
 *   <li><strong>Aggregated results:</strong> Data from all nodes is merged into a single result</li>
 *   <li><strong>Per-node results:</strong> Data is returned separately for each node</li>
 * </ul>
 *
 * <p>The class provides access to:</p>
 * <ul>
 *   <li>Build information</li>
 *   <li>Namespace details</li>
 *   <li>Set information</li>
 *   <li>Secondary index information</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * InfoCommands commands = new InfoCommands(session);
 *
 * // Get all namespaces
 * Set<String> namespaces = commands.namespaces();
 *
 * // Get namespace details
 * Optional<NamespaceDetail> nsDetail = commands.namespaceDetails("test");
 *
 * // Get all secondary indexes
 * List<Sindex> indexes = commands.secondaryIndexes();
 *
 * // Get per-node results
 * Map<Node, List<SetDetail>> setsPerNode = commands.setsPerNode();
 * }</pre>
 *
 * @author Aerospike
 * @since 1.0
 */
public class InfoCommands {
    private final Session session;
    private final InfoParser infoParser = new InfoParser();

    /**
     * Creates a new InfoCommands instance with the specified session.
     *
     * @param session the Aerospike session to use for info commands
     */
    public InfoCommands(Session session) {
        this.session = session;
    }

    /**
     * Gets the build information from all nodes in the cluster.
     *
     * @return a set of build strings from all nodes
     */
    public Set<String>  build() {
        return infoParser.mergeCommaSeparatedLists(session, "build");
    }

    /**
     * Gets the list of namespaces from all nodes in the cluster.
     *
     * @return a set of namespace names from all nodes
     */
    public Set<String> namespaces() {
        return infoParser.mergeCommaSeparatedLists(session, "namespaces");
    }

    /**
     * Gets detailed information about a specific namespace from all nodes.
     * The results are merged into a single NamespaceDetail object.
     *
     * @param namespace the name of the namespace
     * @return an Optional containing the merged namespace details, or empty if not found
     */
    public Optional<NamespaceDetail> namespaceDetails(String namespace) {
        return infoParser.getInfoForSingleItem(session, NamespaceDetail.class, "namespace/"+namespace, true);
    }

    /**
     * Gets detailed information about a specific namespace from each node separately.
     *
     * @param namespace the name of the namespace
     * @return a map of node to optional namespace details
     */
    public Map<Node, Optional<NamespaceDetail>> namespaceDetailsPerNode(String namespace) {
        return infoParser.getInfoForSingleItemPerNode(session, NamespaceDetail.class, "namespace/"+namespace);
    }

    /**
     * Gets information about all secondary indexes from all nodes.
     * The results are merged into a single list.
     *
     * @return a list of merged secondary index information
     */
    public List<Sindex> secondaryIndexes() {
        return secondaryIndexes(true);
    }
    /**
     * Gets information about all secondary indexes from all nodes.
     * The results are merged into a single list.
     *
     * @return a list of merged secondary index information
     */
    public List<Sindex> secondaryIndexes(boolean allowLogging) {
        return infoParser.getInfoForMultipleItems(session, Sindex.class, "sindex-list", allowLogging);
    }

    /**
     * Gets information about all secondary indexes from each node separately.
     *
     * @return a map of node to list of secondary indexes
     */
    public Map<Node, List<Sindex>> secondaryIndexesPerNode() {
        return infoParser.getInfoForMultipleItemsPerNode(session, Sindex.class, "sindex-list");
    }

    /**
     * Gets information about all sets from all nodes.
     * The results are merged into a single list.
     *
     * @return a list of merged set information
     */
    public List<SetDetail> sets() {
        return infoParser.getInfoForMultipleItems(session, SetDetail.class, "sets", true);
    }

    /**
     * Gets information about the specific set from all nodes.
     * The results are merged into a single item.
     *
     * @return an Optional containing the set information if it exists.
     */
    public Optional<SetDetail> set(String name) {
        List<SetDetail> details = sets();
        return details.stream().filter(detail -> detail.getSet().equals(name)).findFirst();
    }

    /**
     * Gets information about all sets from each node separately.
     *
     * @return a map of node to list of set information
     */
    public Map<Node, List<SetDetail>> setsPerNode() {
        return infoParser.getInfoForMultipleItemsPerNode(session, SetDetail.class, "sets");
    }

    /**
     * Gets detailed information about a specific secondary index from all nodes.
     * The results are merged into a single SindexDetail object.
     *
     * @param namespace the namespace containing the index
     * @param indexName the name of the secondary index
     * @return an Optional containing the merged index details, or empty if not found
     */
    public Optional<SindexDetail> secondaryIndexDetails(String namespace, String indexName) {
        return secondaryIndexDetails(namespace, indexName, true);
    }

    public Optional<SindexDetail> secondaryIndexDetails(String namespace, String indexName, boolean allowLogging) {
        return infoParser.getInfoForSingleItem(session, SindexDetail.class, "sindex-stat:namespace=" + namespace + ";indexname=" + indexName, allowLogging);
    }

    /**
     * Gets detailed information about a specific secondary index from each node separately.
     *
     * @param namespace the namespace containing the index
     * @param indexName the name of the secondary index
     * @return a map of node to optional index details
     */
    public Map<Node, Optional<SindexDetail>> secondaryIndexDetailsPerNode(String namespace, String indexName) {
        return infoParser.getInfoForSingleItemPerNode(session, SindexDetail.class, "sindex-stat:namespace=" + namespace + ";indexname=" + indexName);
    }

    /**
     * Gets detailed information about a specific secondary index from all nodes.
     * Convenience method that extracts namespace and index name from the Sindex object.
     *
     * @param index the Sindex object containing namespace and index name information
     * @return an Optional containing the merged index details, or empty if not found
     */
    public Optional<SindexDetail> secondaryIndexDetails(Sindex index) {
        return secondaryIndexDetails(index.getNamespace(), index.getIndexName());
    }

    public Optional<SindexDetail> secondaryIndexDetails(Sindex index, boolean allowLogging) {
        return secondaryIndexDetails(index.getNamespace(), index.getIndexName(), allowLogging);
    }

    /**
     * Gets detailed information about a specific secondary index from each node separately.
     * Convenience method that extracts namespace and index name from the Sindex object.
     *
     * @param index the Sindex object containing namespace and index name information
     * @return a map of node to optional index details
     */
    public Map<Node, Optional<SindexDetail>> secondaryIndexDetailsPerNode(Sindex index) {
        return secondaryIndexDetailsPerNode(index.getNamespace(), index.getIndexName());
    }
}
