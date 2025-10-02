package com.aerospike.client.fluent.exception;

public class GenerationException extends AeroException {
    private static final long serialVersionUID = 1L;
    public GenerationException(int resultCode, String message, boolean inDoubt) {
        super(resultCode, message, inDoubt);
    }

}
