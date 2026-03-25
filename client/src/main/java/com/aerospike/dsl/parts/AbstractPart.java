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
package com.aerospike.dsl.parts;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.query.Filter;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractPart {

    protected Exp.Type expType;
    protected PartType partType;
    protected Exp exp;
    protected Filter filter;
    protected CTX[] ctx;
    protected boolean isPlaceholder;

    protected AbstractPart(PartType partType) {
        this.partType = partType;
        this.exp = null;
    }

    public enum PartType {
        INT_OPERAND,
        FLOAT_OPERAND,
        BOOL_OPERAND,
        STRING_OPERAND,
        LIST_OPERAND,
        MAP_OPERAND,
        LET_OPERAND,
        LET_STRUCTURE,
        WHEN_STRUCTURE,
        EXCLUSIVE_STRUCTURE,
        AND_STRUCTURE,
        OR_STRUCTURE,
        BASE_PATH,
        BIN_PART,
        LIST_PART,
        MAP_PART,
        PATH_OPERAND,
        PATH_FUNCTION,
        METADATA_OPERAND,
        EXPRESSION_CONTAINER,
        VARIABLE_OPERAND,
        PLACEHOLDER_OPERAND,
        FUNCTION_ARGS
    }
}
