package com.aerospike.benchmarks;

public class TransactionalItem {
    private final TransactionalType type;

    public TransactionalItem(TransactionalType type) {
        this.type = type;
    }

    public TransactionalType getType() {
        return type;
    }


}
