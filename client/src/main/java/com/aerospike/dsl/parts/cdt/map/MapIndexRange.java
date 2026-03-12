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

import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.path.BasePath;

public class MapIndexRange extends MapPart {
    private final boolean inverted;
    private final Integer start;
    private final Integer count;

    public MapIndexRange(boolean inverted, Integer start, Integer end) {
        super(MapPartType.INDEX_RANGE);
        this.inverted = inverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static MapIndexRange from(ConditionParser.MapIndexRangeContext ctx) {
        ConditionParser.StandardMapIndexRangeContext indexRange = ctx.standardMapIndexRange();
        ConditionParser.InvertedMapIndexRangeContext invertedIndexRange = ctx.invertedMapIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = Integer.parseInt(range.start().INT().getText());
            Integer end = null;
            if (range.end() != null) {
                end = Integer.parseInt(range.end().INT().getText());
            }

            return new MapIndexRange(isInverted, start, end);
        }
        throw new DslParseException("Could not translate MapIndexRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (inverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return MapExp.getByIndexRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return MapExp.getByIndexRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
