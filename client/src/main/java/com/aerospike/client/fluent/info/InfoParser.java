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
package com.aerospike.client.fluent.info;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aerospike.client.fluent.Log;
import com.aerospike.client.fluent.Node;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.command.Info;

/**
 * Parser for Aerospike info commands that converts raw info strings into structured data.
 *
 * <p>This class provides functionality to parse Aerospike info command responses and convert them
 * into Java objects. It supports both single-item and multiple-item info responses, and can
 * aggregate data from multiple nodes in a cluster.</p>
 *
 * <p>The parser handles two main types of info responses:</p>
 * <ul>
 *   <li><strong>Single-item responses:</strong> Key-value pairs separated by semicolons (e.g., "key1=value1;key2=value2")</li>
 *   <li><strong>Multiple-item responses:</strong> Multiple records separated by semicolons, each containing key-value pairs separated by colons (e.g., "key1=value1:key2=value2;key3=value3:key4=value4")</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * InfoParser parser = new InfoParser();
 *
 * // Parse single item info
 * Optional<NamespaceDetail> namespace = parser.getInfoForSingleItem(session, NamespaceDetail.class, "namespace/test");
 *
 * // Parse multiple items info
 * List<Sindex> indexes = parser.getInfoForMultipleItems(session, Sindex.class, "sindex-list");
 *
 * // Get per-node results
 * Map<Node, Optional<NamespaceDetail>> perNode = parser.getInfoForSingleItemPerNode(session, NamespaceDetail.class, "namespace/test");
 * }</pre>
 *
 * @author Aerospike
 * @since 1.0
 */
public class InfoParser {

    /**
     * Parses a single-item info string into a key-value map.
     *
     * @param infoString the raw info string from Aerospike
     * @return a map of key-value pairs
     */
    private static Map<String, String> parseInfoSingleItem(String infoString) {
        return Arrays.stream(infoString.split(";"))
                .map(kv -> kv.split("=", 2)) // split only at first '='
                .filter(kv -> kv.length == 2)
                .collect(Collectors.toMap(
                        kv -> kv[0].trim(),
                        kv -> kv[1].trim(),
                        (v1, v2) -> v2 // In case of duplicate keys, use the last one
                ));
    }

    /**
     * Parses an Aerospike info string into a list of key-value maps.
     *
     * @param infoString the raw info string from Aerospike
     * @return a list of maps, one per item in the info string
     */
    private static List<Map<String, String>> parseInfoWithMultipleItems(String infoString) {
        if (infoString == null || infoString.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(infoString.split(";"))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .map(InfoParser::parseItemToMap)
                .collect(Collectors.toList());
    }

    /**
     * Parses a single item string into a key-value map.
     *
     * @param item the item string to parse
     * @return a map of key-value pairs
     */
    private static Map<String, String> parseItemToMap(String item) {
        return Arrays.stream(item.split(":"))
                .map(kv -> kv.split("=", 2)) // split only at first '='
                .filter(kv -> kv.length == 2)
                .collect(Collectors.toMap(
                        kv -> kv[0].trim(),
                        kv -> kv[1].trim(),
                        (v1, v2) -> v2 // In case of duplicate keys, use the last one
                ));
    }

    /**
     * Merges values from multiple nodes for a single item.
     *
     * @param <T> the type of the values to merge
     * @param nodeValues list of values from different nodes
     * @return an Optional containing the merged value, or empty if merging fails
     */
    private static <T> Optional<T> mergeNodeValuesSingleItem(List<T> nodeValues) {
        try {
            return Optional.of(StatMerger.merge(nodeValues));
        } catch (Exception e) {
            Log.warn(String.format("mergeNodeValuesSingleItem threw a %s exception: %s. Stack trace shown if DEBUG is set.",
                    e.getClass().getName(), e.getMessage()));
            if (Log.debugEnabled()) {
                e.printStackTrace();
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Merges values from multiple nodes for multiple items.
     * Groups equivalent items by their key fields and merges them.
     *
     * @param <T> the type of the values to merge
     * @param nodeValues list of lists, where each inner list contains items from one node
     * @return a list of merged items
     */
    private static <T> List<T> mergeNodeValuesMultipleItems(List<List<T>> nodeValues) {
        // We have one list per node. Each list contains the items for that node
        // We need to merge the equivalent records from each node
        List<T> allItems = new ArrayList<>();

        // Put all the items into one list
        nodeValues.forEach(allItems::addAll);

        List<T> results = new ArrayList<>();

        while (!allItems.isEmpty()) {
            List<T> itemsToMerge = new ArrayList<>();

            T thisItem = allItems.get(0);
            itemsToMerge.add(thisItem);

            for (int i = 1; i < allItems.size(); i++) {
                T otherItem = allItems.get(i);
                if (KeyComparator.compareKeyFields(thisItem, otherItem)) {
                    itemsToMerge.add(otherItem);
                }
            }
            try {
                results.add(StatMerger.merge(itemsToMerge));
            } catch (Exception e) {
                Log.warn(String.format("mergeNodeValuesMultipleItems threw a %s exception: %s. Stack trace shown if DEBUG is set.",
                        e.getClass().getName(), e.getMessage()));
                if (Log.debugEnabled()) {
                    e.printStackTrace();
                }
                // ignore this error
            }
            allItems.removeAll(itemsToMerge);
        }

        return results;
    }

    /**
     * Merges comma-separated lists from all nodes in the cluster.
     *
     * @param session the Aerospike session
     * @param infoCall the info command to execute
     * @return a set of unique values from all nodes
     */
    public Set<String> mergeCommaSeparatedLists(Session session, String infoCall) {
        Node[] nodes = session.getCluster().getNodes();
        Set<String> values = new HashSet<>();
        for (Node node : nodes) {
            String result = Info.request(node, infoCall);
            String[] resultList = result.split(",");
            for (String aResult : resultList) {
                values.add(aResult);
            }
        }
        return values;
    }

    /**
     * Gets info for multiple items from each node separately.
     *
     * @param <T> the type of object to create from the info data
     * @param session the Aerospike session
     * @param clazz the class to instantiate for each item
     * @param infoCall the info command to execute
     * @return a map of node to list of objects
     */
    public <T> Map<Node, List<T>> getInfoForMultipleItemsPerNode(Session session, Class<T> clazz, String infoCall) {
        Node[] nodes = session.getCluster().getNodes();
        Map<Node, List<T>> allResults = new HashMap<>();
        for (Node node : nodes) {
            List<T> results = new ArrayList<>();
            allResults.put(node, results);
            String result = Info.request(node, infoCall);
            if (Log.debugEnabled()) {
                Log.debug(String.format("Node: %s, info call: %s, result: %s", node, infoCall, result));
            }
            List<Map<String, String>> infoData = parseInfoWithMultipleItems(result);
            for (Map<String, String>item : infoData) {
                results.add(MapToObjectMapper.mapToObject(item, clazz));
            }
        }
        return allResults;
    }

    /**
     * Gets info for multiple items from all nodes and merges the results.
     *
     * @param <T> the type of object to create from the info data
     * @param session the Aerospike session
     * @param clazz the class to instantiate for each item
     * @param infoCall the info command to execute
     * @return a list of merged objects
     */
    public <T> List<T> getInfoForMultipleItems(Session session, Class<T> clazz, String infoCall, boolean allowLogging) {
        Node[] nodes = session.getCluster().getNodes();
        List<List<T>> allResults = new ArrayList<>();
        for (Node node : nodes) {
            List<T> results = new ArrayList<>();
            allResults.add(results);
            String result = Info.request(node, infoCall);
            if (Log.debugEnabled() && allowLogging) {
                Log.debug(String.format("Node: %s, info call: %s, result: %s", node, infoCall, result));
            }
            List<Map<String, String>> infoData = parseInfoWithMultipleItems(result);
            for (Map<String, String>item : infoData) {
                results.add(MapToObjectMapper.mapToObject(item, clazz));
            }
        }
        return mergeNodeValuesMultipleItems(allResults);
    }

    /**
     * Gets info for a single item from each node separately.
     *
     * @param <T> the type of object to create from the info data
     * @param session the Aerospike session
     * @param clazz the class to instantiate
     * @param infoCall the info command to execute
     * @return a map of node to optional object
     */
    public <T> Map<Node, Optional<T>> getInfoForSingleItemPerNode(Session session, Class<T> clazz, String infoCall) {
        Node[] nodes = session.getCluster().getNodes();
        Map<Node, Optional<T>> allResults = new HashMap<>();
        for (Node node : nodes) {
            String result = Info.request(node, infoCall);
            if (Log.debugEnabled()) {
                Log.debug(String.format("Node: %s, info call: %s, result: %s", node, infoCall, result));
            }
            Map<String, String> infoData = parseInfoSingleItem(result);
            allResults.put(node, Optional.of(MapToObjectMapper.mapToObject(infoData, clazz)));
        }
        return allResults;
    }

    /**
     * Gets info for a single item from all nodes and merges the results.
     *
     * @param <T> the type of object to create from the info data
     * @param session the Aerospike session
     * @param clazz the class to instantiate
     * @param infoCall the info command to execute
     * @return an optional containing the merged object, or empty if no data is available
     */
    public <T> Optional<T> getInfoForSingleItem(Session session, Class<T> clazz, String infoCall, boolean allowLogging) {
        Node[] nodes = session.getCluster().getNodes();
        List<T> allResults = new ArrayList<>();
        for (Node node : nodes) {
            String result = Info.request(node, infoCall);
            if (Log.debugEnabled() && allowLogging) {
                Log.debug(String.format("Node: %s, info call: %s, result: %s", node, infoCall, result));
            }
            Map<String, String> infoData = parseInfoSingleItem(result);
            allResults.add(MapToObjectMapper.mapToObject(infoData, clazz));
        }
        return mergeNodeValuesSingleItem(allResults);
    }
}
