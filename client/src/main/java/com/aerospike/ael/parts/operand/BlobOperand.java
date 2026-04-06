package com.aerospike.ael.parts.operand;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.client.sdk.exp.Exp;

import lombok.Getter;

@Getter
public class BlobOperand extends AbstractPart implements ParsedValueOperand {

    private final byte[] value;

    public BlobOperand(byte[] value) {
        super(PartType.BLOB_OPERAND);
        this.value = value;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
