package com.aerospike.client.fluent.dsl;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.query.Filter;

/**
 * Result of parsing a DSL expression.
 * Contains either an Aerospike expression (Exp) or a Filter for secondary index usage.
 */
public class ParseResult {
    private final Filter filter;
    private final Exp exp;
    
    public ParseResult(Filter filter, Exp exp) {
        this.filter = filter;
        this.exp = exp;
    }
    
    public Exp getExp() {
        return exp;
    }
    
    public Filter getFilter() {
        return filter;
    }
    
    public boolean hasFilter() {
        return filter != null;
    }
}

