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

import com.aerospike.ael.ConditionParser;

import static com.aerospike.ael.util.ParsingUtils.requireIntValueIdentifier;

import com.aerospike.ael.DslParseException;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;

public class ListValueRange extends ListPart {
    private final boolean isInverted;
    private final Integer start;
    private final Integer end;

    public ListValueRange(boolean isInverted, Integer start, Integer end) {
        super(ListPartType.VALUE_RANGE);
        this.isInverted = isInverted;
        this.start = start;
        this.end = end;
    }

    public static ListValueRange from(ConditionParser.ListValueRangeContext ctx) {
        ConditionParser.StandardListValueRangeContext valueRange = ctx.standardListValueRange();
        ConditionParser.InvertedListValueRangeContext invertedValueRange = ctx.invertedListValueRange();

        if (valueRange != null || invertedValueRange != null) {
            ConditionParser.ValueRangeIdentifierContext range =
                    valueRange != null ? valueRange.valueRangeIdentifier() : invertedValueRange.valueRangeIdentifier();
            boolean isInverted = valueRange == null;

            Integer startValue = requireIntValueIdentifier(range.valueIdentifier(0));

            Integer endValue = null;
            if (range.valueIdentifier(1) != null) {
                endValue = requireIntValueIdentifier(range.valueIdentifier(1));
            }

            return new ListValueRange(isInverted, startValue, endValue);
        }
        throw new DslParseException("Could not translate ListValueRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        Exp startExp = Exp.val(start);
        Exp endExp = end != null ? Exp.val(end) : null;

        return ListExp.getByValueRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
