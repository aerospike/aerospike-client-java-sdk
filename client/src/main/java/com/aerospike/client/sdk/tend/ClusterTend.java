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
package com.aerospike.client.sdk.tend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.ClusterDefinition;
import com.aerospike.client.sdk.Host;
import com.aerospike.client.sdk.Loggers;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.discovery.AddressTranslator;
import com.aerospike.client.sdk.discovery.Endpoint;
import com.aerospike.client.sdk.discovery.PassThroughAddressTranslator;
import com.aerospike.client.sdk.discovery.PortMapAddressTranslator;
import com.aerospike.client.sdk.discovery.StaticMapAddressTranslator;
import com.aerospike.client.sdk.discovery.StaticTranslationConfig;
import com.aerospike.client.sdk.discovery.TranslationMissMode;
import com.aerospike.client.sdk.util.Util;

/**
 * Cluster tend thread.
 */
public class ClusterTend implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Loggers.TEND);

    private final Cluster cluster;
    private final ClusterDefinition def;
    private volatile Thread tendThread;
    private final HashMap<String,Node> nodesMap;
    private final ConcurrentLinkedDeque<ConnectionRecover> recoverQueue;
    private final AddressTranslator addressTranslator;
    private final Map<String,String> translationMisses;
    private final LongAdder recoverCount;
    private final AtomicBoolean translatorClosed = new AtomicBoolean();
    private volatile Host[] seeds;
    private int tendCount;
    private volatile int invalidNodeCount;
    private int viewChange;
    private final boolean notifyViewChange;
    private volatile boolean valid;

    public ClusterTend(Cluster cluster, Host[] seeds) {
        this.cluster = cluster;
        this.def = cluster.getClusterDefinition();
        this.seeds = seeds;
        this.nodesMap = new HashMap<String,Node>();
        this.recoverCount = new LongAdder();
        this.recoverQueue = new ConcurrentLinkedDeque<ConnectionRecover>();
        this.translationMisses = (def.getAddressTranslator() != null)? new ConcurrentHashMap<>() : null;
        this.addressTranslator = (def.getAddressTranslator() != null)?
            def.getAddressTranslator() : new StaticMapAddressTranslator(def.getIpMap());
        this.notifyViewChange = overridesViewChange(addressTranslator);
    }

    private static boolean overridesViewChange(AddressTranslator translator) {
        try {
            return translator.getClass().getMethod("onClusterViewChanged", int.class)
                .getDeclaringClass() != AddressTranslator.class;
        }
        catch (NoSuchMethodException e) {
            return false;
        }
    }

    public void runThread() {
        // Tend cluster until all nodes identified.
        waitTillStabilized(def.isFailIfNotConnected());

        if (log.isDebugEnabled()) {
            for (Host host : seeds) {
                log.atDebug()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Add seed " + host);
            }
        }

        // Add other nodes as seeds, if they don't already exist.
        Node[] nodes = cluster.getNodes();
        ArrayList<Host> seedsToAdd = new ArrayList<Host>(nodes.length);
        for (Node node : nodes) {
            Host host = node.getHost();
            if (! findSeed(host)) {
                seedsToAdd.add(host);
            }
        }

        if (seedsToAdd.size() > 0) {
            addSeeds(seedsToAdd.toArray(new Host[seedsToAdd.size()]));
        }

        // Run cluster tend thread.
        valid = true;
        tendThread = new Thread(this);
        tendThread.setName("tend");
        tendThread.setDaemon(true);
        tendThread.start();
    }

    public final void run() {
        while (valid) {
            // Tend cluster.
            try {
                tend(false, false);
            }
            catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.atWarn()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log("Cluster tend failed: " + Util.getErrorMessage(e));
                }
            }
            // Sleep between polling intervals.
            Util.sleep(def.getTendInterval());
        }
        cluster.closeSeedRefresher();
        closeTranslator();
    }

    public void forceSingleNode() {
        // Initialize tendThread, but do not start it.
        valid = true;
        tendThread = new Thread(this);

        // Validate first seed.
        Host seed = seeds[0];
        NodeValidator nv = new NodeValidator();
        Node node = null;

        try {
            node = nv.seedNode(cluster, seed, null);
        }
        catch (Throwable e) {
            throw new AerospikeException("Seed " + seed + " failed: " + e.getMessage(), e);
        }

        node.createMinConnections();

        // Add seed node to nodes.
        HashMap<String,Node> nodesToAdd = new HashMap<String,Node>(1);
        nodesToAdd.put(node.getName(), node);
        addNodes(nodesToAdd);

        // Initialize partitionMaps.
        Node[] nodes = cluster.getNodes();
        Peers peers = new Peers(nodes.length + 16);
        node.refreshPartitions(peers);

        // Set partition maps for all namespaces to point to same node.
        for (Partitions partitions : cluster.getPartitionMap().values()) {
            for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
                int max = nodeArray.length();

                for (int i = 0; i < max; i++) {
                    nodeArray.set(i, node);
                }
            }
        }
    }

    /**
     * Tend the cluster until it has stabilized and return control.
     * This helps avoid initial database request timeout issues when
     * a large number of threads are initiated at client startup.
     */
    private void waitTillStabilized(boolean failIfNotConnected) {
        // Tend now requests partition maps in same iteration as the nodes
        // are added, so there is no need to call tend twice anymore.
        tend(failIfNotConnected, true);

        if (cluster.getNodes().length == 0) {
            String message = "Cluster seed(s) failed";

            if (failIfNotConnected) {
                throw new AerospikeException(message);
            }
            else {
                if (log.isWarnEnabled()) {
                    log.atWarn()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log(message);
                }
            }
        }
    }

    /**
     * Check health of all nodes in the cluster.
     */
    private void tend(boolean failIfNotConnected, boolean isInit) {
        // All node additions/deletions are performed in tend thread.
        // Initialize tend iteration node statistics.
        Node[] nodes = cluster.getNodes();
        Node[] nodesBefore = nodes;
        Peers peers = new Peers(nodes.length + 16);

        // Clear node reference counts.
        for (Node node : nodes) {
            node.tendReset();
        }

        // If active nodes don't exist, seed cluster.
        if (nodes.length == 0) {
            // Must copy array reference for copy on write semantics to work.
            Host[] seedArray = seeds;

            if (! isInit) {
                cluster.refreshSeedRefresher();
            }
            seedNode(seedArray, peers, failIfNotConnected);

            // Reset nodes because seedNode changes them.
            nodes = cluster.getNodes();

            // Abort cluster init if all peers of the seed are not reachable and failIfNotConnected is true.
            if (isInit && failIfNotConnected && nodes.length == 1 && peers.getInvalidCount() > 0) {
                peers.clusterInitError();
            }
        }
        else {
            // Refresh all known nodes.
            for (Node node : nodes) {
                node.refresh(peers);
            }

            for (Node node : nodes) {
                node.refreshAddress(peers);
            }

            // Refresh peers when necessary.
            if (peers.genChanged) {
                // Refresh peers for all nodes that responded the first time even if only one node's peers changed.
                peers.refreshCount = 0;

                for (Node node : nodes) {
                    node.refreshPeers(peers);
                }

                // Handle nodes changes determined from refreshes.
                findNodesToRemove(peers);
            }

            // Add peer nodes to cluster before removing the nodes they replace.
            boolean added = peers.nodes.size() > 0;

            if (added) {
                addNodes(peers.nodes);
            }

            // Remove nodes in a batch.
            if (peers.nodesToRemove.size() > 0) {
                removeNodes(peers);
            }

            if (added) {
                refreshPeers(peers);
            }
        }

        nodes = cluster.getNodes();

        if (notifyViewChange && ! isInit && (nodes != nodesBefore || peers.addressChanged)) {
            clusterViewChanged();
        }

        invalidNodeCount += peers.getInvalidCount();

        // Refresh partition map when necessary.
        for (Node node : nodes) {
            if (node.isPartitionChanged()) {
                node.refreshPartitions(peers);
            }

            if (node.isRebalanceChanged()) {
                node.refreshRacks();
            }
        }

        tendCount++;

        // Balance connections every 30 tend iterations.
        if (tendCount % 30 == 0) {
            for (Node node : nodes) {
                node.balanceConnections();
            }
        }

        // Reset connection error window for all nodes every connErrorWindow tend iterations.
        if (tendCount % def.getNumTendIntervalsInErrorWindow() == 0) {
            for (Node node : nodes) {
                node.resetErrorRate();
            }
        }

        if (! isInit) {
            cluster.tendSeedRefresher(tendCount);
        }

        processRecoverQueue();
    }

    private void clusterViewChanged() {
        viewChange++;

        try {
            addressTranslator.onClusterViewChanged(viewChange);
        }
        catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.atWarn()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Address translator " + translatorName() + " onClusterViewChanged failed: " +
                        Util.getErrorMessage(e));
            }
        }
    }

    private boolean seedNode(Host[] seedArray, Peers peers, boolean failIfNotConnected) {
        Throwable[] exceptions = null;
        NodeValidator nv = new NodeValidator();

        for (int i = 0; i < seedArray.length; i++) {
            Host seed = seedArray[i];

            try {
                Node node = nv.seedNode(cluster, seed, peers);

                if (node != null) {
                    addSeedAndPeers(node, peers);
                    return true;
                }
            }
            catch (Throwable e) {
                peers.fail(seed);

                if (seed.tlsName != null && def.getTlsBuilder() == null) {
                    // Fail immediately for known configuration errors like this.
                    throw new AerospikeException.Connection("Seed host tlsName '" + seed.tlsName +
                        "' defined but client TLS not enabled", e);
                }

                // Store exception and try next seed.
                if (failIfNotConnected) {
                    if (exceptions == null) {
                        exceptions = new Exception[seedArray.length];
                    }
                    exceptions[i] = e;
                }
                else if (e instanceof AerospikeException.AlternateAddressMissing) {
                    if (log.isErrorEnabled()) {
                        log.atError()
                            .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                            .log("Seed " + seed + " failed: " + e.getMessage());
                    }
                }
                else {
                    if (log.isWarnEnabled()) {
                        log.atWarn()
                            .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                            .log("Seed " + seed + " failed: " + Util.getErrorMessage(e));
                    }
                }
            }
        }

        // No seeds valid. Use fallback node if it exists.
        if (nv.fallback != null) {
            // When a fallback is used, peers refreshCount is reset to zero.
            // refreshCount should always be one at this point.
            peers.refreshCount = 1;
            addSeedAndPeers(nv.fallback, peers);
            return true;
        }

        if (failIfNotConnected) {
            if (exceptions != null) {
                for (Throwable ex : exceptions) {
                    if (ex instanceof AerospikeException.AlternateAddressMissing) {
                        throw (AerospikeException.AlternateAddressMissing)ex;
                    }
                }
            }

            StringBuilder sb = new StringBuilder(500);
            sb.append("Failed to connect to ["+ seedArray.length +"] host(s): ");
            sb.append(System.lineSeparator());

            for (int i = 0; i < seedArray.length; i++) {
                sb.append(seedArray[i]);
                sb.append(' ');

                Throwable ex = exceptions == null ? null : exceptions[i];

                if (ex != null) {
                    sb.append(ex.getMessage());
                    sb.append(System.lineSeparator());
                }
            }
            throw new AerospikeException.Connection(sb.toString());
        }
        return false;
    }

    private void addSeedAndPeers(Node seed, Peers peers) {
        seed.createMinConnections();
        nodesMap.clear();

        addNodes(seed, peers);

        if (peers.nodes.size() > 0) {
            refreshPeers(peers);
        }
    }

    private void refreshPeers(Peers peers) {
        // Iterate until peers have been refreshed and all new peers added.
        while (true) {
            // Copy peer node references to array.
            Node[] nodeArray = new Node[peers.nodes.size()];
            int count = 0;

            for (Node node : peers.nodes.values()) {
                nodeArray[count++] = node;
            }

            // Reset peer nodes.
            peers.nodes.clear();

            // Refresh peers of peers in order retrieve the node's peersCount
            // which is used in RefreshPartitions(). This call might add even
            // more peers.
            for (Node node : nodeArray) {
                node.refreshPeers(peers);
            }

            boolean added = peers.nodes.size() > 0;

            if (added) {
                // Add new peer nodes to cluster before removing the nodes they replace.
                addNodes(peers.nodes);
            }

            if (peers.nodesToRemove.size() > 0) {
                removeNodes(peers);
            }

            if (! added) {
                break;
            }
        }
    }

    public void setSeeds(Host[] seeds) {
        this.seeds = seeds;
    }

    private void addSeeds(Host[] hosts) {
        // Use copy on write semantics.
        Host[] seedArray = new Host[seeds.length + hosts.length];
        int count = 0;

        // Add existing seeds.
        for (Host seed : seeds) {
            seedArray[count++] = seed;
        }

        // Add new seeds
        for (Host host : hosts) {
            if (log.isDebugEnabled()) {
                log.atDebug()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Add seed " + host);
            }
            seedArray[count++] = host;
        }

        // Replace nodes with copy.
        seeds = seedArray;
    }

    private boolean findSeed(Host search) {
        for (Host seed : seeds) {
            if (seed.equals(search)) {
                return true;
            }
        }
        return false;
    }

    private void findNodesToRemove(Peers peers) {
        int refreshCount = peers.refreshCount;
        Node[] nodes = cluster.getNodes();

        for (Node node : nodes) {
            if (! node.isActive()) {
                // Inactive nodes must be removed.
                peers.removeNode(node);
                continue;
            }

            if (refreshCount == 0 && node.getFailures() >= 5) {
                // All node info requests failed and this node had 5 consecutive failures.
                // Remove node.  If no nodes are left, seeds will be tried in next cluster
                // tend iteration.
                peers.removeNode(node);
                continue;
            }

            if (nodes.length > 1 && refreshCount >= 1 && node.getReferenceCount() == 0) {
                // Node is not referenced by other nodes.
                // Check if node responded to info request.
                if (node.getFailures() == 0) {
                    // Node is alive, but not referenced by other nodes.  Check if mapped.
                    if (! findNodeInPartitionMap(node)) {
                        // Node doesn't have any partitions mapped to it.
                        // There is no point in keeping it in the cluster.
                        peers.removeNode(node);
                    }
                }
                else {
                    // Node not responding. Remove it.
                    peers.removeNode(node);
                }
            }
        }
    }

    private boolean findNodeInPartitionMap(Node filter) {
        for (Partitions partitions : cluster.getPartitionMap().values()) {
            for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
                int max = nodeArray.length();

                for (int i = 0; i < max; i++) {
                    Node node = nodeArray.get(i);
                    // Use reference equality for performance.
                    if (node == filter) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void addNodes(Node seed, Peers peers) {
        // Add all nodes at once to avoid copying entire array multiple times.
        // Create temporary nodes array.
        Node[] nodeArray = new Node[peers.nodes.size() + 1];
        int count = 0;

        // Add seed.
        nodeArray[count++] = seed;
        addNode(seed);

        // Add peers.
        for (Node peer : peers.nodes.values()) {
            nodeArray[count++] = peer;
            addNode(peer);
        }

        // Replace nodes with copy.
        cluster.setNodes(nodeArray);
    }

    /**
     * Add nodes using copy on write semantics.
     */
    private void addNodes(HashMap<String,Node> nodesToAdd) {
        // Add all nodes at once to avoid copying entire array multiple times.
        // Create temporary nodes array.
        Node[] nodes = cluster.getNodes();
        Node[] nodeArray = new Node[nodes.length + nodesToAdd.size()];
        int count = 0;

        // Add existing nodes.
        for (Node node : nodes) {
            nodeArray[count++] = node;
        }

        // Add new nodes.
        for (Node node : nodesToAdd.values()) {
            nodeArray[count++] = node;
            addNode(node);
        }

        // Replace nodes with copy.
        cluster.setNodes(nodeArray);
    }

    private void addNode(Node node) {
        if (log.isInfoEnabled()) {
            log.atInfo()
                .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                .log("Add node " + node);
        }

        nodesMap.put(node.getName(), node);
    }

    private void removeNodes(Peers peers) {
        // There is no need to delete nodes from partitionWriteMap because the nodes
        // have already been set to inactive. Further connection requests will result
        // in an exception and a different node will be tried.
        Set<Node> nodesToRemove = peers.nodesToRemove;

        // Flush node metrics and remove nodes atomically with respect to the metrics thread.
        cluster.metricsNodeClose(nodesToRemove, () -> {
            // Cleanup node resources.
            for (Node node : nodesToRemove) {
                // Remove node from map unless a replacement node already took its name.
                if (nodesMap.get(node.getName()) == node) {
                    nodesMap.remove(node.getName());
                }
                node.close();
            }

            // Remove all nodes at once to avoid copying entire array multiple times.
            removeNodesCopy(nodesToRemove);
            peers.removeNodes.clear();
            nodesToRemove.clear();

            // Departed nodes can't be full partition owners.
            for (Partitions partitions : cluster.getPartitionMap().values()) {
                partitions.removeFullOwners(nodesToRemove);
            }
        });
    }

    /**
     * Remove nodes using copy on write semantics.
     */
    private void removeNodesCopy(Set<Node> nodesToRemove) {
        // Create temporary nodes array.
        // Since nodes are only marked for deletion using node references in the nodes array,
        // and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
        // in nodesToRemove exist.  Therefore, we know the final array size.
        Node[] nodes = cluster.getNodes();
        Node[] nodeArray = new Node[nodes.length - nodesToRemove.size()];
        int count = 0;

        // Add nodes that are not in remove list.
        for (Node node : nodes) {
            if (nodesToRemove.contains(node)) {
                if (log.isInfoEnabled()) {
                    log.atInfo()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log("Remove node " + node);
                }
            }
            else {
                nodeArray[count++] = node;
            }
        }

        // Do sanity check to make sure assumptions are correct.
        if (count < nodeArray.length) {
            if (log.isWarnEnabled()) {
                log.atWarn()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Node remove mismatch. Expected " + nodeArray.length + " Received " + count);
            }
            // Resize array.
            Node[] nodeArray2 = new Node[count];
            System.arraycopy(nodeArray, 0, nodeArray2, 0, count);
            nodeArray = nodeArray2;
        }

        // Replace nodes with copy.
        cluster.setNodes(nodeArray);
    }

    public final void recoverConnection(ConnectionRecover cs) {
        // Many cloud providers encounter performance problems when sockets are
        // closed by the client when the server still has data left to write.
        // The solution is to shutdown the socket and give the server time to
        // respond before closing the socket.
        //
        // Put connection on a queue for later closing.
        if (cs.isComplete()) {
            return;
        }

        // Do not let queue get out of control.
        if (recoverCount.longValue() < 10000L) {
            recoverCount.increment();
            recoverQueue.offerLast(cs);
        }
        else {
            cs.abort();
        }
    }

    private void processRecoverQueue() {
        ConnectionRecover last = recoverQueue.peekLast();

        if (last == null) {
            return;
        }

        byte[] buf = new byte[8192];
        ConnectionRecover cs;

        while ((cs = recoverQueue.pollFirst()) != null) {
            if (cs.drain(buf)) {
                recoverCount.decrement();
            }
            else {
                recoverQueue.offerLast(cs);
            }

            if (cs == last) {
                break;
            }
        }
    }

    /**
     * Translate a server advertised address. The advertised TLS name is always kept.
     * Throw {@link AerospikeException.TranslationMiss} on a strict miss.
     */
    public final Host translateAddress(String nodeName, String tlsName, String host, int port) {
        Endpoint.SourceList sourceList = def.isUseServicesAlternate()?
            Endpoint.SourceList.ALTERNATE : Endpoint.SourceList.STANDARD;

        return translate(addressTranslator, def.getTranslationOnMiss(),
            new Endpoint(nodeName, tlsName, host, port, sourceList), translationMisses);
    }

    Host translate(AddressTranslator translator, Endpoint advertised) {
        return translate(translator, TranslationMissMode.PASS_THROUGH, advertised);
    }

    Host translate(AddressTranslator translator, TranslationMissMode onMiss, Endpoint advertised) {
        return translate(translator, onMiss, advertised, null);
    }

    /**
     * Translate an advertised address. On a pass-through miss, warn once per node and advertised
     * address when {@code misses} is not null.
     */
    private Host translate(AddressTranslator translator, TranslationMissMode onMiss, Endpoint advertised,
        Map<String,String> misses) {
        Optional<Host> result = translator.translate(advertised);

        if (result == null) {
            throw new AerospikeException("Address translator " + translatorName(translator) +
                " returned null for " + advertised);
        }

        if (! result.isPresent() || ! isValidPort(result.get().port)) {
            if (onMiss == TranslationMissMode.STRICT) {
                throw new AerospikeException.TranslationMiss(advertised, translatorName(translator));
            }

            if (misses != null) {
                String address = advertised.host + ':' + advertised.port;

                if (! address.equals(misses.put(String.valueOf(advertised.nodeName), address)) && log.isWarnEnabled()) {
                    log.atWarn()
                        .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                        .log("Address translator " + translatorName(translator) + " has no mapping for node " +
                        advertised.nodeName + " advertised address " + address + " in the " +
                        (advertised.sourceList == Endpoint.SourceList.ALTERNATE ? "alternate" : "standard") +
                        " list. Connecting to the advertised address because translation.on_miss is pass_through");
                }
            }
            return new Host(advertised.host, advertised.tlsName, advertised.port);
        }

        Host host = result.get();

        if (! Objects.equals(host.tlsName, advertised.tlsName)) {
            host = new Host(host.name, advertised.tlsName, host.port);
        }
        return host;
    }

    static boolean isValidPort(int port) {
        return port >= 1 && port <= 65535;
    }

    public final String translatorName() {
        return translatorName(addressTranslator);
    }

    private static String translatorName(AddressTranslator translator) {
        if (translator instanceof PassThroughAddressTranslator) {
            return StaticTranslationConfig.PASS_THROUGH;
        }
        if (translator instanceof PortMapAddressTranslator) {
            return StaticTranslationConfig.PORT_MAP;
        }
        return (translator instanceof StaticMapAddressTranslator)? StaticTranslationConfig.STATIC_MAP : translator.getClass().getName();
    }

    public final boolean getNotifyViewChange() {
        return notifyViewChange;
    }

    public final void interruptTendSleep() {
        // Interrupt tendThread's sleep(), so node refreshes will be performed sooner.
        tendThread.interrupt();
    }

    public final boolean isActive() {
        return valid;
    }


    /**
     * Return map of current nodes.
     */
    public HashMap<String, Node> getNodesMap() {
        return nodesMap;
    }

    /**
     * Return connection recoverQueue size. The queue contains connections that have timed out and
     * need to be drained before returning the connection to a connection pool. The recoverQueue
     * is only used when
     * {@link com.aerospike.client.sdk.policy.ResolvedSettings#getWaitForSocketResponseAfterCallFailsMs()}
     * is true.
     * <p>
     * Since recoverQueue is a linked list where the size() calculation is expensive, a separate
     * counter is used to track recoverQueue.size().
     */
    public final int getRecoverQueueSize() {
        return recoverCount.intValue();
    }

    /**
     * Return count of add node failures in the most recent cluster tend iteration.
     */
    public final int getInvalidNodeCount() {
        return invalidNodeCount;
    }

    /**
     * Return if tend thread is current running.
     */
    public final boolean isTendRunning() {
        return tendThread != null && tendThread.isAlive();
    }

    public void closeTranslator() {
        if (! translatorClosed.compareAndSet(false, true)) {
            return;
        }
        closeTranslator(addressTranslator, def);
    }

    /**
     * Close a translator and log, not throw, a close failure. Used directly when cluster
     * construction fails before the {@code ClusterTend} that owns the translator exists.
     */
    public static void closeTranslator(AddressTranslator translator, ClusterDefinition def) {
        if (translator == null) {
            return;
        }

        try {
            translator.close();
        }
        catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.atWarn()
                    .addKeyValue(Cluster.CONTEXT, def.getClusterName())
                    .log("Address translator " + translatorName(translator) + " close failed: " + Util.getErrorMessage(e));
            }
        }
    }

    public final void close() {
        // Stop cluster tend thread.
        valid = false;
        Thread thread = tendThread;
        if (thread == null) {
            return;
        }
        thread.interrupt();
        if (!thread.isAlive()) {
            return;
        }
        try {
            thread.join((long) def.getTendInterval() + def.getTendTimeout());
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
