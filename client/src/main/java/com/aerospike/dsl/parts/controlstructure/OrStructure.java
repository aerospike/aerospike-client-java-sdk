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
package com.aerospike.dsl.parts.controlstructure;

import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;

import java.util.List;

@Getter
public class OrStructure extends AbstractPart {

    private final List<ExpressionContainer> operands;

    public OrStructure(List<ExpressionContainer> operands) {
        super(PartType.OR_STRUCTURE);
        this.operands = operands;
    }
}
