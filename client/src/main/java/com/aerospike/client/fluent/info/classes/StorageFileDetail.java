package com.aerospike.client.fluent.info.classes;

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