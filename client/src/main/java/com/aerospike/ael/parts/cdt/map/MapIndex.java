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
package com.aerospike.ael.parts.cdt.map;

import com.aerospike.dsl.ConditionParser;

import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;

import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.MapExp;

public class MapIndex extends MapPart {
    private final int index;

    public MapIndex(int index) {
        super(MapPartType.INDEX);
        this.index = index;
    }

    public static MapIndex from(ConditionParser.MapIndexContext ctx) {
        return new MapIndex(parseSignedInt(ctx.signedInt()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return MapExp.getByIndex(cdtReturnType, valueType, Exp.val(index),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapIndex(index);
    }
}
