package com.aerospike.client.fluent.exception;

class SecurityException extends AeroException {
    private static final long serialVersionUID = 1L;
    public SecurityException(int resultCode, String message, boolean inDoubt) {
        super(resultCode, message, inDoubt);
    }
}