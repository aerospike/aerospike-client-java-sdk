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
package com.aerospike.dsl.parts.path;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;

import com.aerospike.dsl.parts.ExpressionContainer;
import lombok.Getter;

@Getter
public class PathFunction extends AbstractPart {

    private final PathFunctionType pathFunctionType;
    private final ReturnParam returnParam;
    private final Exp.Type binType;

    public PathFunction(PathFunctionType pathFunctionType, ReturnParam returnParam, Exp.Type binType) {
        super(PartType.PATH_FUNCTION);
        this.pathFunctionType = pathFunctionType;
        this.returnParam = returnParam;
        this.binType = binType;
    }

    public static Exp.Type castTypeToExpType(CastType castType) {
        return switch (castType) {
            case INT -> Exp.Type.INT;
            case FLOAT -> Exp.Type.FLOAT;
        };
    }

    /**
     * Returns the source {@link Exp.Type} for a cast — the type the value must
     * already have before the cast is applied (e.g., casting to INT requires a FLOAT source).
     */
    public static Exp.Type castSourceExpType(CastType castType) {
        return switch (castType) {
            case INT -> Exp.Type.FLOAT;
            case FLOAT -> Exp.Type.INT;
        };
    }

    public static ExpressionContainer.ExprPartsOperation castTypeToOperation(CastType castType) {
        return switch (castType) {
            case INT -> ExpressionContainer.ExprPartsOperation.TO_INT;
            case FLOAT -> ExpressionContainer.ExprPartsOperation.TO_FLOAT;
        };
    }

    public enum ReturnParam {
        VALUE,
        INDEX,
        RANK,
        COUNT,
        NONE,
        EXISTS,
        REVERSE_INDEX,
        REVERSE_RANK,
        KEY_VALUE,
        UNORDERED_MAP,
        ORDERED_MAP,
        KEY
    }

    public enum PathFunctionType {
        GET,
        COUNT,
        SIZE,
        CAST
    }

    public enum CastType {
        INT,
        FLOAT
    }
}
