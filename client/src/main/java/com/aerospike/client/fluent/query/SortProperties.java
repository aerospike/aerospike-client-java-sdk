 package com.aerospike.client.fluent.query;

public class SortProperties {
    private final String name;
    private final SortDir sortDir;
    private final boolean caseInsensitive;
    
    public SortProperties(String name, SortDir sortDir, boolean caseInsensitive) {
        this.name = name;
        this.sortDir = sortDir;
        this.caseInsensitive = caseInsensitive;
    }

    public String getName() {
        return name;
    }

    public SortDir getSortDir() {
        return sortDir;
    }

    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }
}
