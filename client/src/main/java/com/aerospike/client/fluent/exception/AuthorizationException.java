package com.aerospike.client.fluent.exception;

class AuthorizationException extends SecurityException {
    private static final long serialVersionUID = 1L;
    public AuthorizationException(int resultCode, String message, boolean inDoubt) {
        super(resultCode, message, inDoubt);
    }
}