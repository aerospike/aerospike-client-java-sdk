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
package com.aerospike.ael.parts.cdt.list;

import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;
import static com.aerospike.ael.util.ParsingUtils.subtractNullable;

import com.aerospike.ael.DslParseException;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.dsl.ConditionParser;

public class ListIndexRange extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;

    public ListIndexRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.INDEX_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static ListIndexRange from(ConditionParser.ListIndexRangeContext ctx) {
        ConditionParser.StandardListIndexRangeContext indexRange = ctx.standardListIndexRange();
        ConditionParser.InvertedListIndexRangeContext invertedIndexRange = ctx.invertedListIndexRange();

        if (indexRange != null || invertedIndexRange != null) {
            ConditionParser.IndexRangeIdentifierContext range =
                    indexRange != null ? indexRange.indexRangeIdentifier() : invertedIndexRange.indexRangeIdentifier();
            boolean isInverted = indexRange == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.end() != null) {
                end = parseSignedInt(range.end().signedInt());
            }

            return new ListIndexRange(isInverted, start, end);
        }
        throw new DslParseException("Could not translate ListIndexRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByIndexRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return ListExp.getByIndexRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
