package com.aerospike.ael.parts.controlstructure;

import lombok.Getter;

import java.util.List;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.operand.LetOperand;

@Getter
public class LetStructure extends AbstractPart {

    private final List<LetOperand> operands;

    public LetStructure(List<LetOperand> operands) {
        super(PartType.LET_STRUCTURE);
        this.operands = operands;
    }
}
