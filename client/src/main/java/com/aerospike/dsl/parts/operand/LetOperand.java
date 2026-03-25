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

import com.aerospike.dsl.parts.AbstractPart;
import lombok.Getter;
import lombok.Setter;

@Getter
public class LetOperand extends AbstractPart {

    private final String string;
    @Setter
    private AbstractPart part;
    private final boolean isLastPart;

    public LetOperand(AbstractPart part, String string) {
        super(PartType.LET_OPERAND);
        this.string = string;
        this.part = part;
        this.isLastPart = false;
    }

    public LetOperand(AbstractPart part, boolean isLastPart) {
        super(PartType.LET_OPERAND);
        this.string = null;
        this.part = part;
        this.isLastPart = isLastPart;
    }
}
