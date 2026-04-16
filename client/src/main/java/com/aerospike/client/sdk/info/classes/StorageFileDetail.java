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
package com.aerospike.client.sdk.info.classes;

public class StorageFileDetail {
    private String filePath;
    private long usedBytes;
    private long freeWblocks;
    private long readErrors;
    private long writeQ;
    private long writes;
    private long partialWrites;
    private long defragQ;
    private long defragReads;
    private long defragWrites;
    private long defragPartialWrites;
    private long age;
    @Override
    public String toString() {
        return "FileDetail [filePath=" + filePath + ", usedBytes=" + usedBytes + ", freeWblocks=" + freeWblocks
                + ", readErrors=" + readErrors + ", writeQ=" + writeQ + ", writes=" + writes + ", partialWrites="
                + partialWrites + ", defragQ=" + defragQ + ", defragReads=" + defragReads + ", defragWrites="
                + defragWrites + ", defragPartialWrites=" + defragPartialWrites + ", age=" + age + "]";
    }
    public String getFilePath() {
        return filePath;
    }
    public long getUsedBytes() {
        return usedBytes;
    }
    public long getFreeWblocks() {
        return freeWblocks;
    }
    public long getReadErrors() {
        return readErrors;
    }
    public long getWriteQ() {
        return writeQ;
    }
    public long getWrites() {
        return writes;
    }
    public long getPartialWrites() {
        return partialWrites;
    }
    public long getDefragQ() {
        return defragQ;
    }
    public long getDefragReads() {
        return defragReads;
    }
    public long getDefragWrites() {
        return defragWrites;
    }
    public long getDefragPartialWrites() {
        return defragPartialWrites;
    }
    public long getAge() {
        return age;
    }
}