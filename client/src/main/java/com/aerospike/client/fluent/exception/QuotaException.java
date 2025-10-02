package com.aerospike.client.fluent.exception;

class QuotaException extends AeroException {
    private static final long serialVersionUID = 1L;
    public QuotaException(int resultCode, String message, boolean inDoubt) {
        super(resultCode, message, inDoubt);
    }
}