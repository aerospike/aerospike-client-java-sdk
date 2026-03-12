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

import java.util.List;

import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;

import lombok.Getter;

@Getter
public class BasePath extends AbstractPart {

    private final BinPart binPart;
    private final List<AbstractPart> cdtParts;

    public BasePath(BinPart binPart, List<AbstractPart> cdtParts) {
        super(PartType.BASE_PATH);
        this.binPart = binPart;
        this.cdtParts = cdtParts;
    }

    // Bin type is determined by the base path's first element
    public Exp.Type getBinType() {
        if (!cdtParts.isEmpty()) {
            return switch (cdtParts.get(0).getPartType()) {
                case MAP_PART -> Exp.Type.MAP;
                case LIST_PART -> Exp.Type.LIST;
                default -> null;
            };
        }
        return binPart.getExpType();
    }
}
