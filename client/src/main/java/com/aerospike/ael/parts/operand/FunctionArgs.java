package com.aerospike.ael.parts.operand;

import lombok.Getter;

import java.util.List;

import com.aerospike.ael.parts.AbstractPart;

@Getter
public class FunctionArgs extends AbstractPart {

    private final List<AbstractPart> operands;

    public FunctionArgs(List<AbstractPart> operands) {
        super(PartType.FUNCTION_ARGS);
        this.operands = List.copyOf(operands);
    }
}
