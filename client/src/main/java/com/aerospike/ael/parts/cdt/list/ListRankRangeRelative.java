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

import com.aerospike.dsl.ConditionParser;

import static com.aerospike.ael.util.ParsingUtils.*;

import com.aerospike.ael.DslParseException;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;

public class ListRankRangeRelative extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;
    private final Object relative;

    public ListRankRangeRelative(boolean isInverted, Integer start, Integer end, Object relative) {
        super(ListPartType.RANK_RANGE_RELATIVE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
        this.relative = relative;
    }

    public static ListRankRangeRelative from(ConditionParser.ListRankRangeRelativeContext ctx) {
        ConditionParser.StandardListRankRangeRelativeContext rankRangeRelative = ctx.standardListRankRangeRelative();
        ConditionParser.InvertedListRankRangeRelativeContext invertedRankRangeRelative = ctx.invertedListRankRangeRelative();

        if (rankRangeRelative != null || invertedRankRangeRelative != null) {
            ConditionParser.RankRangeRelativeIdentifierContext range =
                    rankRangeRelative != null ? rankRangeRelative.rankRangeRelativeIdentifier()
                            : invertedRankRangeRelative.rankRangeRelativeIdentifier();
            boolean isInverted = rankRangeRelative == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.relativeRankEnd().end() != null) {
                end = parseSignedInt(range.relativeRankEnd().end().signedInt());
            }

            Object relativeValue = null;
            if (range.relativeRankEnd().relativeValue() != null) {
                relativeValue = ParsingUtils.parseValueIdentifier(
                        range.relativeRankEnd().relativeValue().valueIdentifier());
            }

            return new ListRankRangeRelative(isInverted, start, end, relativeValue);
        }
        throw new DslParseException("Could not translate ListRankRangeRelative from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp relativeExp = objectToExp(relative);

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp,
                    Exp.bin(basePath.getBinPart().getBinName(),
                            basePath.getBinType()), context);
        }

        return ListExp.getByValueRelativeRankRange(cdtReturnType, startExp, relativeExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
