package com.aerospike.client.fluent.query;

public interface ResettablePagination {
    int currentPage();
    int maxPages();
    void setPageTo(int newPage);
}
