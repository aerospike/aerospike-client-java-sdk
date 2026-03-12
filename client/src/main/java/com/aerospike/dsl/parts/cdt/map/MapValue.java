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
package com.aerospike.dsl.parts.cdt.map;

import static com.aerospike.dsl.util.ParsingUtils.unquote;

import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.parts.path.BasePath;

public class MapValue extends MapPart {
    private final Object value;

    public MapValue(Object value) {
        super(MapPartType.VALUE);
        this.value = value;
    }

    public static MapValue from(ConditionParser.MapValueContext ctx) {
        Object mapValue = null;
        if (ctx.valueIdentifier().NAME_IDENTIFIER() != null) {
            mapValue = ctx.valueIdentifier().NAME_IDENTIFIER().getText();
        } else if (ctx.valueIdentifier().QUOTED_STRING() != null) {
            mapValue = unquote(ctx.valueIdentifier().QUOTED_STRING().getText());
        } else if (ctx.valueIdentifier().INT() != null) {
            mapValue = Integer.parseInt(ctx.valueIdentifier().INT().getText());
        }
        return new MapValue(mapValue);
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        Exp valueExp = switch (valueType) {
            case BOOL -> Exp.val((Boolean) value);
            case STRING -> Exp.val((String) value);
            case FLOAT -> Exp.val((Float) value);
            default -> Exp.val((Integer) value); // for getByValue the default is INT
        };

        return MapExp.getByValue(cdtReturnType, valueExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.mapValue(Value.get(value));
    }
}
