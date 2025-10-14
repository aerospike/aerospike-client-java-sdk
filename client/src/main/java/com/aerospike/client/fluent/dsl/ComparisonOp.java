package com.aerospike.client.fluent.dsl;

public enum ComparisonOp {
    EQ("=="),    // Equal
    NE("!="),    // Not Equal
    GT(">"),    // Greater Than
    LT("<"),    // Less Than
    GTE(">="),   // Greater Than or Equal
    LTE(">=");   // Less Than or Equal
    
    private final String value;
    private ComparisonOp(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
} 