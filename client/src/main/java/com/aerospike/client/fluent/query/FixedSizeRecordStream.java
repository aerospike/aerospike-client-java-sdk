package com.aerospike.client.fluent.query;

import java.util.Arrays;
import java.util.List;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.BatchRecord;

public class FixedSizeRecordStream implements RecordStreamImpl, Sortable, ResettablePagination {
    private final RecordResult[] records;
    private final int pageSize;
    private final int numPages;
    private int currentPage = -1;
    private List<SortProperties> sortInfo;
    private int index = 0;

    public FixedSizeRecordStream(Key[] keys, Record[] records, long limit, int pageSize, List<SortProperties> sortProperties, boolean respondAllKeys) {
        this(convertToRecordResults(keys, records), limit, pageSize, sortProperties, respondAllKeys);
    }
    public FixedSizeRecordStream(List<BatchRecord> records, long limit, int pageSize, List<SortProperties> sortProperties) {
        // Do not filter by null if !respondAllKeys as it must be done by the caller, in case there is a partition filter
        RecordResult[] recs = records.stream().map(RecordResult::new).toArray(RecordResult[]::new);

        if (limit > 0 && limit < recs.length) {
            recs = Arrays.copyOf(recs, (int)limit);
        }
        this.records = recs;
        this.pageSize = pageSize;
        this.sortInfo = sortProperties;
        applySort();
        
        this.numPages = (int)(pageSize > 0 ? (recs.length + pageSize - 1) / pageSize : 1);
    }
    
    public FixedSizeRecordStream(RecordResult[] records, long limit, int pageSize, List<SortProperties> sortProperties, boolean respondAllKeys) {
        RecordResult[] recs;
        if (respondAllKeys) {
            recs = records;
        }
        else {
            recs = Arrays.stream(records)
                .filter(rec -> rec.recordOrNull() != null)
                .toArray(RecordResult[]::new);
        }
        if (limit < recs.length) {
            recs = Arrays.copyOf(records, (int)limit);
        }
        this.records = recs;
        this.pageSize = pageSize;
        this.sortInfo = sortProperties;
        applySort();

        this.numPages = pageSize > 0 ? (records.length + pageSize - 1) / pageSize : 1;
    }

    // Called from the constructor, must be static
    private static RecordResult[] convertToRecordResults(Key[] keys, Record[] records) {
        RecordResult[] recordResults = new RecordResult[keys.length];
        for (int i = 0; i < keys.length; i++) {
            recordResults[i] = new RecordResult(keys[i], records[i]);
        }
        return recordResults;
    }
    private void applySort() {
        if (sortInfo != null && !sortInfo.isEmpty()) {
            Arrays.sort(this.records, new RecordComparator(sortInfo));
        }
        this.currentPage = -1;
        this.index = 0;
    }
    @Override
    public void sortBy(List<SortProperties> sortPropertyList) {
        this.sortInfo = sortPropertyList;
        this.applySort();
    }
    @Override
    public void sortBy(SortProperties sortProperty) {
        this.sortInfo = List.of(sortProperty);
        this.applySort();
    }

    @Override
    public boolean hasMorePages() {
        boolean result;
        if (currentPage == -1) {
            currentPage = 0;
            result = true;
        }
        else {
            result = (++currentPage) < numPages;
        }
        return result;
    }

    @Override
    public boolean hasNext() {
        if (index >= records.length) {
            return false;
        }
        long pageOfIndex = pageSize == 0 ? 0 : (index / pageSize);
        // If the current page is -1, they are not using pagination.
        return currentPage == -1 || pageOfIndex == currentPage;
    }

    @Override
    public RecordResult next() {
        if (index >=0 && index < records.length) {
            return records[index++];
        }
        return null;
    }

    @Override
    public void close() {
    }
    @Override
    public int currentPage() {
        return currentPage + 1;
    }
    @Override
    public int maxPages() {
        return numPages;
    }
    @Override
    public void setPageTo(int newPage) {
        if (newPage < 1 || newPage > numPages) {
            throw new IllegalArgumentException(String.format(
                    "setPageTo must take page number in the range of 1 to %,d, not %,d",
                    numPages, newPage));
        }
        currentPage = newPage-1;
        index = currentPage * pageSize;
    }

}
