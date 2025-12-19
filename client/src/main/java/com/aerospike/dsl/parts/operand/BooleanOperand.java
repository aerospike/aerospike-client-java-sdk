package com.aerospike.dsl.parts.operand;

import static com.aerospike.dsl.parts.AbstractPart.PartType.BOOL_OPERAND;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;

import lombok.Getter;

@Getter
public class BooleanOperand extends AbstractPart implements ParsedValueOperand {

    // Keeping the boxed type for interface compatibility
    private final Boolean value;

    public BooleanOperand(Boolean value) {
        // Setting parent type
        super(BOOL_OPERAND);
        this.value = value;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
