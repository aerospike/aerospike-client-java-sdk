/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;

@Getter
public class PlaceholderOperand extends AbstractPart {

    private final int index;

    public PlaceholderOperand(int index) {
        super(PartType.PLACEHOLDER_OPERAND);
        super.isPlaceholder = true;
        this.index = index;
    }

    /**
     * Resolve placeholder's value based on index in {@link PlaceholderValues} and create a corresponding operand using
     * {@link OperandFactory#createOperand(Object)}
     *
     * @param values Values to be matched with placeholders by index
     * @return Created {@link AbstractPart} operand
     */
    public AbstractPart resolve(PlaceholderValues values) {
        Object resolvedValue = values.getValue(index);
        return OperandFactory.createOperand(resolvedValue);
    }
}
