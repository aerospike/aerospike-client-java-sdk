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
package com.aerospike.dsl.parts.cdt.list;

import static com.aerospike.dsl.util.ParsingUtils.parseSignedInt;
import static com.aerospike.dsl.util.ParsingUtils.subtractNullable;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.cdt.ListReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.path.BasePath;

public class ListRankRange extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer count;

    public ListRankRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.RANK_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.count = subtractNullable(end, start);
    }

    public static ListRankRange from(ConditionParser.ListRankRangeContext ctx) {
        ConditionParser.StandardListRankRangeContext rankRange = ctx.standardListRankRange();
        ConditionParser.InvertedListRankRangeContext invertedRankRange = ctx.invertedListRankRange();

        if (rankRange != null || invertedRankRange != null) {
            ConditionParser.RankRangeIdentifierContext range =
                    rankRange != null ? rankRange.rankRangeIdentifier() : invertedRankRange.rankRangeIdentifier();
            boolean isInverted = rankRange == null;

            Integer start = parseSignedInt(range.start().signedInt());
            Integer end = null;
            if (range.end() != null) {
                end = parseSignedInt(range.end().signedInt());
            }

            return new ListRankRange(isInverted, start, end);
        }
        throw new DslParseException("Could not translate ListRankRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        if (count == null) {
            return ListExp.getByRankRange(cdtReturnType, startExp, Exp.bin(basePath.getBinPart().getBinName(),
                    basePath.getBinType()), context);
        }

        return ListExp.getByRankRange(cdtReturnType, startExp, Exp.val(count),
                Exp.bin(basePath.getBinPart().getBinName(),
                        basePath.getBinType()), context);
    }
}
