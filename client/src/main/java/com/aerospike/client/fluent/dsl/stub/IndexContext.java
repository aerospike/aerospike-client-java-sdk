package com.aerospike.client.fluent.dsl.stub;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.aerospike.client.fluent.dsl.Index;

/**
 * TEMPORARY STUB - Replace when library is fixed
 */
public class IndexContext {
    private final Set<Index> availableIndexes;
    
    public IndexContext(Set<Index> availableIndexes) {
        this.availableIndexes = availableIndexes;
    }
    
    public static IndexContext empty() {
        return new IndexContext(Set.of());
    }
    
    public static IndexContext of(String namespace, Set<Index> indexes) {
        // Filter indexes by namespace if needed, for now just return all
        return new IndexContext(new HashSet<>(indexes));
    }
    
    public Collection<Index> getIndexes() {
        return availableIndexes;
    }
    
    public boolean hasIndex(String binName) {
        return availableIndexes.stream().anyMatch(idx -> idx.getBinName().equals(binName));
    }
}

