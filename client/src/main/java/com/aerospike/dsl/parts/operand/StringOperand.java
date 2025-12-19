package com.aerospike.dsl.parts.operand;

import java.util.Base64;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;

import lombok.Getter;
import lombok.Setter;

@Getter
public class StringOperand extends AbstractPart implements ParsedValueOperand {

    private final String value;
    @Setter
    private boolean isBlob = false;

    public StringOperand(String string) {
        super(PartType.STRING_OPERAND);
        this.value = string;
    }

    @Override
    public Exp getExp() {
        if (isBlob) {
            byte[] byteValue = Base64.getDecoder().decode(value);
            return Exp.val(byteValue);
        }
        return Exp.val(value);
    }
}
