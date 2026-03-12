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

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.path.BasePath;

public class MapValueRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer end;

    public MapValueRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.VALUE_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.end = end;
    }

    public static MapValueRange from(ConditionParser.MapValueRangeContext ctx) {
        ConditionParser.StandardMapValueRangeContext valueRange = ctx.standardMapValueRange();
        ConditionParser.InvertedMapValueRangeContext invertedValueRange = ctx.invertedMapValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            Integer startValue = Integer.parseInt(range.valueIdentifier(0).INT().getText());

            Integer endValue = null;
            if (range.valueIdentifier(1) != null && range.valueIdentifier(1).INT() != null) {
                endValue = Integer.parseInt(range.valueIdentifier(1).INT().getText());
            }

            return new MapValueRange(isInverted, startValue, endValue);
        }
        throw new DslParseException("Could not translate MapValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return MapExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
