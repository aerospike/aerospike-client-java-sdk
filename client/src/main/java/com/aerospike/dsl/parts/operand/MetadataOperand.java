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

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.dsl.parts.ExpressionContainer;

import lombok.Getter;

@Getter
public class MetadataOperand extends ExpressionContainer {

    private final String functionName;
    private final Integer parameter;

    public MetadataOperand(String functionName) {
        super();
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
        this.parameter = null;
    }

    public MetadataOperand(String functionName, int parameter) {
        super();
        this.partType = PartType.METADATA_OPERAND;
        this.functionName = functionName;
        this.parameter = parameter;
    }

    private Exp constructMetadataExp(String functionName, Integer parameter) {
        return switch (functionName) {
            case "deviceSize", "recordSize", "memorySize" -> Exp.recordSize();
            case "digestModulo" -> Exp.digestModulo(parameter);
            case "isTombstone" -> Exp.isTombstone();
            case "keyExists" -> Exp.keyExists();
            case "lastUpdate" -> Exp.lastUpdate();
            case "sinceUpdate" -> Exp.sinceUpdate();
            case "setName" -> Exp.setName();
            case "ttl" -> Exp.ttl();
            case "voidTime" -> Exp.voidTime();
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    @Override
    public Exp getExp() {
        return constructMetadataExp(functionName, parameter);
    }

    public MetadataReturnType getMetadataType() {
        return switch (functionName) {
            case "deviceSize",
                 "memorySize",
                 "recordSize",
                 "digestModulo",
                 "lastUpdate",
                 "sinceUpdate",
                 "ttl",
                 "voidTime" -> MetadataReturnType.INT;
            case "isTombstone",
                 "keyExists" -> MetadataReturnType.BOOL;
            case "setName" -> MetadataReturnType.STRING;
            default -> throw new IllegalArgumentException("Unknown metadata function: " + functionName);
        };
    }

    public enum MetadataReturnType {
        INT,
        STRING,
        BOOL,
    }
}
