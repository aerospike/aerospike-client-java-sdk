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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.aerospike.client.sdk.Node;

/**
 * Concurrent bounded LIFO stack of connections.
 * <p>
 * The standard library concurrent stack, ConcurrentLinkedDequeue, will not suffice
 * because it's not bounded and it's size() method is too expensive.
 */
public final class Pool {
    private final Node node;
    private final ReentrantLock lock;
    private final ArrayDeque<Connection> queue;
    private final AtomicInteger total;  // total connections: inUse + inPool
    private int minSize;
    private int maxSize;

    /**
     * Initialize connection pool.
     */
    public Pool(Node node, int minSize, int maxSize) {
        this.node = node;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.lock = new ReentrantLock(false);
        this.queue = new ArrayDeque<>(maxSize);
        this.total = new AtomicInteger();
    }

    /**
     * Resize minimum connections.
     */
    public void setMinSize(int minSize) {
        final ReentrantLock lock = this.lock;
        lock.lock();

        try {
            this.minSize = minSize;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Resize maximum connections.
     */
    public void setMaxSize(int maxSize) {
        final ReentrantLock lock = this.lock;
        lock.lock();

        try {
            int excess = queue.size() - maxSize;

            if (excess > 0) {
                // Close oldest connections.
                for (int i = 0; i < excess; i++) {
                    Connection conn = queue.pollLast();

                    if (conn == null) {
                        break;
                    }

                    closeIdle(conn);
                }
            }
            this.maxSize = maxSize;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Insert connection at head of stack.
     */
    public boolean offer(Connection conn) {
        if (conn == null) {
            throw new NullPointerException();
        }

        final ReentrantLock lock = this.lock;
        lock.lock();

        try {
            if (queue.size() >= maxSize) {
                return false;
            }

            queue.addFirst(conn);
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Pop connection from head of stack.
     */
    public Connection poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();

        try {
            return queue.pollFirst();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Close connections that are idle for more than maxSocketIdle up to count.
     */
    public void closeIdle(int count) {
        while (count > 0) {
            // Lock on each iteration to give fairness to other
            // threads polling for connections.
            Connection conn;
            final ReentrantLock lock = this.lock;
            lock.lock();

            try {
                // The oldest connection is at tail.
                conn = queue.peekLast();

                if (conn == null) {
                    return;
                }

                if (isConnCurrentTrim(conn.getLastUsed())) {
                    return;
                }

                conn = queue.pollLast();
            }
            finally {
                lock.unlock();
            }

            // Close connection outside of lock.
            closeIdle(conn);
            count--;
        }
    }

    private boolean isConnCurrentTrim(long lastUsed) {
        return (System.nanoTime() - lastUsed) <= node.cluster.getClusterDefinition().getMaxSocketIdleNanosTrim();
    }

    private void closeIdle(Connection conn) {
        total.getAndDecrement();
        node.closeIdleConnection(conn);
    }

    /**
     * Return item count.
     */
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();

        try {
            return queue.size();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Get minimum connection count.
     */
    public int getMinSize() {
        return minSize;
    }

    /**
     * Get maximum connection count.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Increment total connections count.
     */
    public int incrTotal() {
        return total.getAndIncrement();
    }

    /**
     * Decrement total connections count.
     */
    public int decrTotal() {
        return total.getAndDecrement();
    }

    /**
     * Return count of connections that might be closed.
     */
    public int excess() {
        return total.get() - minSize;
    }
}
