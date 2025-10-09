package com.aerospike.client.fluent.query;

import java.util.Arrays;
import java.util.List;

import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.Record;
import com.aerospike.client.fluent.RecordResult;

public class FixedSizeRecordStream implements RecordStreamImpl, Sortable, ResettablePagination {
    private final RecordResult[] records;
    private final int pageSize;
    private final int numPages;
    private int currentPage = -1;
    private List<SortProperties> sortInfo;
    private int index = 0;
    private long limit;

    public FixedSizeRecordStream(Key[] keys, Record[] records, long limit, int pageSize, List<SortProperties> sortProperties) {
        this(convertToKeyRecords(keys, records), limit, pageSize, sortProperties);
    }
    public FixedSizeRecordStream(RecordResult[] records, long limit, int pageSize, List<SortProperties> sortProperties) {
        // TODO: Consider whether to get NULLs back for
        // a) missing records
        // b) filtered out records.
        // Also need to apply the limit.
        // For now, just filter them out.
        this.records = Arrays.stream(records)
                .filter(rec -> rec.recordOrNull() != null)
                .toArray(RecordResult[]::new);
        this.pageSize = pageSize;
        this.sortInfo = sortProperties;
        applySort();

        this.numPages = pageSize > 0 ? (records.length + pageSize - 1) / pageSize : 1;
    }

    // Called from the constructor, must be static
    private static RecordResult[] convertToKeyRecords(Key[] keys, Record[] records) {
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
