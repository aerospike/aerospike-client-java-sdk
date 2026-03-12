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
package com.aerospike.client.fluent.info.classes;

import com.aerospike.client.fluent.info.annotations.Key;
import com.aerospike.client.fluent.info.annotations.Named;

public class SetDetail {
    @Key
    @Named("ns")
    private String namespace;
    @Key
    private String set;
    private long objects;
    private long tombstones;
    private long dataUsedBytes;
    private long truncateLut;
    private int sindexes;
    private boolean indexPopulating;
    private boolean truncating;
    private int defaultReadTouchTtlPct;
    private int defaultTtl;
    private boolean disableEviction;
    private boolean enableIndex;
    private int stopWritesCount;
//        @Named("stop-writes-size")
//        private long stopWritesSize;
    public String getNamespace() {
        return namespace;
    }
    public String getSet() {
        return set;
    }
    public long getObjects() {
        return objects;
    }
    public long getTombstones() {
        return tombstones;
    }
    public long getDataUsedBytes() {
        return dataUsedBytes;
    }
    public long getTruncateLut() {
        return truncateLut;
    }
    public int getSindexes() {
        return sindexes;
    }
    public boolean isIndexPopulating() {
        return indexPopulating;
    }
    public boolean isTruncating() {
        return truncating;
    }
    public int getDefaultReadTouchTtlPct() {
        return defaultReadTouchTtlPct;
    }
    public int getDefaultTtl() {
        return defaultTtl;
    }
    public boolean isDisableEviction() {
        return disableEviction;
    }
    public boolean isEnableIndex() {
        return enableIndex;
    }
    public int getStopWritesCount() {
        return stopWritesCount;
    }
    @Override
    public String toString() {
        return "SetDetail [namespace=" + namespace + ", set=" + set + ", objects=" + objects + ", tombstones="
                + tombstones + ", dataUsedBytes=" + dataUsedBytes + ", sindexes=" + sindexes
                + ", defaultReadTouchTtlPct=" + defaultReadTouchTtlPct + ", defaultTtl=" + defaultTtl
                + ", disableEviction=" + disableEviction + ", enableIndex=" + enableIndex + ", stopWritesCount="
                + stopWritesCount + "]";
    }
}