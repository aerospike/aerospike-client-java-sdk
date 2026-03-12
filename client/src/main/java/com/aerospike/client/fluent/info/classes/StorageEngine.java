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

import java.util.List;

import com.aerospike.client.fluent.info.annotations.Named;

public class StorageEngine {
    private StorageEngineType type;
    @Named("file")
    private List<StorageFileDetail> files;
    private boolean cacheReplicaWrites;
    private boolean coldStartEmpty;
    private boolean commitToDevice;
    private CompressionAlgorithm compression;
    private long compressionAcceleration;
    private long compressionLevel;
    private long defragLwmPct;
    private long defragQueueMin;
    private long defragSleep;
    private long defragStartupMinimum;
    private boolean directFiles;
    private boolean disableOdsync;
    private boolean enableBenchmarksStorage;
    private String encryptionKeyFile;
    private String encryptionOldKeyFile;
    private long evictUsedPct;
    private long filesize;
    private long flushMaxMs;
    private long flushSize;
    private long maxWriteCache;
    private long postWriteCache;
    private boolean readPageCache;
    private boolean serializeTombRaider;
    private boolean sindexStartupDeviceScan;
    private long stopWritesAvailPct;
    private long stopWritesUsedPct;
    private long tombRaiderSleep;
    public StorageEngineType getType() {
        return type;
    }
    public List<StorageFileDetail> getFiles() {
        return files;
    }
    public boolean isCacheReplicaWrites() {
        return cacheReplicaWrites;
    }
    public boolean isColdStartEmpty() {
        return coldStartEmpty;
    }
    public boolean isCommitToDevice() {
        return commitToDevice;
    }
    public CompressionAlgorithm getCompression() {
        return compression;
    }
    public long getCompressionAcceleration() {
        return compressionAcceleration;
    }
    public long getCompressionLevel() {
        return compressionLevel;
    }
    public long getDefragLwmPct() {
        return defragLwmPct;
    }
    public long getDefragQueueMin() {
        return defragQueueMin;
    }
    public long getDefragSleep() {
        return defragSleep;
    }
    public long getDefragStartupMinimum() {
        return defragStartupMinimum;
    }
    public boolean isDirectFiles() {
        return directFiles;
    }
    public boolean isDisableOdsync() {
        return disableOdsync;
    }
    public boolean isEnableBenchmarksStorage() {
        return enableBenchmarksStorage;
    }
    public String getEncryptionKeyFile() {
        return encryptionKeyFile;
    }
    public String getEncryptionOldKeyFile() {
        return encryptionOldKeyFile;
    }
    public long getEvictUsedPct() {
        return evictUsedPct;
    }
    public long getFilesize() {
        return filesize;
    }
    public long getFlushMaxMs() {
        return flushMaxMs;
    }
    public long getFlushSize() {
        return flushSize;
    }
    public long getMaxWriteCache() {
        return maxWriteCache;
    }
    public long getPostWriteCache() {
        return postWriteCache;
    }
    public boolean isReadPageCache() {
        return readPageCache;
    }
    public boolean isSerializeTombRaider() {
        return serializeTombRaider;
    }
    public boolean isSindexStartupDeviceScan() {
        return sindexStartupDeviceScan;
    }
    public long getStopWritesAvailPct() {
        return stopWritesAvailPct;
    }
    public long getStopWritesUsedPct() {
        return stopWritesUsedPct;
    }
    public long getTombRaiderSleep() {
        return tombRaiderSleep;
    }


}
// file[0]=/opt/aerospike/data/test.dat
