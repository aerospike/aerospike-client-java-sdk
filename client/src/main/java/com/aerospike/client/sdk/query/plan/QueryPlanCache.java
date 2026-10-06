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
package com.aerospike.client.sdk.query.plan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.sdk.Cluster;
import com.aerospike.client.sdk.Node;
import com.aerospike.client.sdk.command.Info;
import com.aerospike.client.sdk.util.Util;

/**
 * Cross-query cache of server query plans (approach A1).
 *
 * <p>Correctness never depends on a cached plan: every node re-applies the full filter, and a
 * pinned index that is gone or not readable fails with 201/203, which invalidates the entry. The
 * cache only decides whether the explain round trip can be skipped.</p>
 *
 * <p>Entries are dropped when they expire, when execute reports 201/203, and when any node's
 * per-namespace {@code sindex_gen} changes (index created, dropped or became readable).</p>
 */
public final class QueryPlanCache {

    public record Key(String namespace, String set, String ael, int whereFlags, String indexHint) {
    }

    private record Entry(QueryPlan plan, long expiresNanos) {
    }

    private final Cluster cluster;
    private final LinkedHashMap<Key, Entry> map;
    private final Map<String, Map<String, Long>> nodeGens = new ConcurrentHashMap<>();
    private final Set<String> polledNamespaces = ConcurrentHashMap.newKeySet();
    private volatile boolean pollerStarted;

    public QueryPlanCache(Cluster cluster) {
        this.cluster = cluster;
        this.map = new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key, Entry> eldest) {
                return size() > QueryPlanSettings.getCacheSize();
            }
        };
    }

    public QueryPlan get(Key key) {
        Entry e;

        synchronized (map) {
            e = map.get(key);

            if (e != null && System.nanoTime() - e.expiresNanos > 0) {
                map.remove(key);
                e = null;
            }
        }

        if (e == null) {
            QueryPlanStats.cacheMisses.increment();
            return null;
        }

        QueryPlanStats.cacheHits.increment();
        return e.plan;
    }

    public void put(Key key, QueryPlan plan) {
        long ttlNanos = QueryPlanSettings.getCacheTtlMs() * 1_000_000L;

        synchronized (map) {
            map.put(key, new Entry(plan, System.nanoTime() + ttlNanos));
        }

        if (cluster == null) {
            return;
        }

        if (polledNamespaces.add(key.namespace())) {
            refreshGens(key.namespace());
        }
        startPoller();
    }

    public void invalidate(Key key) {
        if (key == null) {
            return;
        }

        synchronized (map) {
            if (map.remove(key) != null) {
                QueryPlanStats.cacheInvalidations.increment();
            }
        }
    }

    public void invalidateNamespace(String namespace) {
        synchronized (map) {
            int before = map.size();

            map.keySet().removeIf(k -> Objects.equals(k.namespace(), namespace));
            QueryPlanStats.cacheInvalidations.add(before - map.size());
        }
    }

    public void clear() {
        synchronized (map) {
            map.clear();
        }
    }

    public int size() {
        synchronized (map) {
            return map.size();
        }
    }

    private void startPoller() {
        if (pollerStarted) {
            return;
        }

        synchronized (this) {
            if (pollerStarted) {
                return;
            }
            pollerStarted = true;
        }

        cluster.startVirtualThread(() -> {
            while (cluster.isActive()) {
                Util.sleep((int)QueryPlanSettings.getGenPollMs());

                for (String ns : polledNamespaces) {
                    if (refreshGens(ns)) {
                        invalidateNamespace(ns);
                    }
                }
            }
        });
    }

    /**
     * Reads {@code sindex_gen} from every node for a namespace.
     *
     * @return {@code true} if any node's generation differs from the last poll
     */
    private boolean refreshGens(String namespace) {
        Map<String, Long> latest = new HashMap<>();

        for (Node node : cluster.getNodes()) {
            try {
                String resp = Info.request(node, "namespace/" + namespace);
                Long gen = parseGen(resp);

                if (gen != null) {
                    latest.put(node.getName(), gen);
                }
            }
            catch (Throwable t) {
                // A node that cannot answer is treated as unchanged; execute errors still invalidate.
            }
        }

        QueryPlanStats.genPolls.increment();

        Map<String, Long> prev = nodeGens.put(namespace, latest);
        return prev != null && ! prev.equals(latest);
    }

    static Long parseGen(String resp) {
        if (resp == null) {
            return null;
        }

        for (String kv : resp.split(";")) {
            if (kv.startsWith("sindex_gen=")) {
                return Long.parseLong(kv.substring("sindex_gen=".length()).trim());
            }
        }
        return null;
    }
}
