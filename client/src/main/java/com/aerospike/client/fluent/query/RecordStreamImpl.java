package com.aerospike.client.fluent.query;

import java.util.Iterator;

import com.aerospike.client.fluent.RecordResult;

public interface RecordStreamImpl extends Iterator<RecordResult>{
    boolean hasMorePages();
    boolean hasNext();
    RecordResult next();
    void close();
}
