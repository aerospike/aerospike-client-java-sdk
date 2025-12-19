package com.aerospike.dsl.parts.operand;

import java.util.List;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;

import lombok.Getter;

@Getter
public class ListOperand extends AbstractPart implements ParsedValueOperand {

    private final List<Object> value;

    public ListOperand(List<Object> list) {
        super(PartType.LIST_OPERAND);
        this.value = list;
    }

    @Override
    public Exp getExp() {
        return Exp.val(value);
    }
}
