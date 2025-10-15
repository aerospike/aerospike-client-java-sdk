package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.query.Filter;
import com.aerospike.client.fluent.query.IndexType;

/**
 * Represents an Aerospike secondary index
 */
public class Index {
    private final String namespace;
    private final String binName;
    private final IndexType indexType;
    private final Filter filter;
    private final double binValuesRatio;
    
    private Index(Builder builder) {
        this.namespace = builder.namespace;
        this.binName = builder.binName;
        this.indexType = builder.indexType;
        this.filter = builder.filter;
        this.binValuesRatio = builder.binValuesRatio;
    }
    
    public Index(String binName, IndexType indexType, Filter filter) {
        this(binName, indexType, filter, 1.0);
    }
    
    public Index(String binName, IndexType indexType, Filter filter, double binValuesRatio) {
        this.namespace = null;
        this.binName = binName;
        this.indexType = indexType;
        this.filter = filter;
        this.binValuesRatio = binValuesRatio;
    }
    
    public String getNamespace() {
        return namespace;
    }
    
    public String getBinName() {
        return binName;
    }
    
    public IndexType getIndexType() {
        return indexType;
    }
    
    public Filter getFilter() {
        return filter;
    }
    
    public double getBinValuesRatio() {
        return binValuesRatio;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String namespace;
        private String binName;
        private IndexType indexType;
        private Filter filter;
        private double binValuesRatio = 1.0;
        
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }
        
        public Builder bin(String binName) {
            this.binName = binName;
            return this;
        }
        
        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }
        
        public Builder filter(Filter filter) {
            this.filter = filter;
            return this;
        }
        
        public Builder binValuesRatio(double binValuesRatio) {
            this.binValuesRatio = binValuesRatio;
            return this;
        }
        
        public Index build() {
            return new Index(this);
        }
    }
}

