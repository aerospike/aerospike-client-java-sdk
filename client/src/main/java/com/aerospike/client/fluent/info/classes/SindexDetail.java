package com.aerospike.client.fluent.info.classes;

public class SindexDetail {
    private long entries;
    private long usedBytes;
    private long entriesPerBval;
    private long entriesPerRec;
    private int loadPct;
    private long loadTime;
    private long statGcRecs;
    public long getEntries() {
        return entries;
    }
    public long getUsedBytes() {
        return usedBytes;
    }
    public long getEntriesPerBval() {
        return entriesPerBval;
    }
    public long getEntriesPerRec() {
        return entriesPerRec;
    }
    public int getLoadPct() {
        return loadPct;
    }
    public long getLoadTime() {
        return loadTime;
    }
    public long getStatGcRecs() {
        return statGcRecs;
    }
    @Override
    public String toString() {
        return "SindexDetail [entries=" + entries + ", usedBytes=" + usedBytes + ", entriesPerBval=" + entriesPerBval
                + ", entriesPerRec=" + entriesPerRec + ", loadPct=" + loadPct + "]";
    }
    
}
