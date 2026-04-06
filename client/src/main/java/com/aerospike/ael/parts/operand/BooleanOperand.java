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
package com.aerospike.ael.parts.operand;

import static com.aerospike.ael.parts.AbstractPart.PartType.BOOL_OPERAND;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.client.sdk.exp.Exp;

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
