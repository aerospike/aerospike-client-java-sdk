package com.aerospike.client.fluent.exception;

class AuthenticationException extends SecurityException {
    private static final long serialVersionUID = 1L;
    public AuthenticationException(int resultCode, String message, boolean inDoubt) {
        super(resultCode, message, inDoubt);
    }
}