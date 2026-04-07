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

import java.util.List;

import com.aerospike.ael.DslParseException;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;
import com.aerospike.ael.ConditionParser;

public class ListValueList extends ListPart {
    private final boolean isInverted;
    private final List<?> valueList;

    public ListValueList(boolean isInverted, List<?> valueList) {
        super(ListPartType.VALUE_LIST);
        this.isInverted = isInverted;
        this.valueList = valueList;
    }

    public static ListValueList from(ConditionParser.ListValueListContext ctx) {
        ConditionParser.StandardListValueListContext valueList = ctx.standardListValueList();
        ConditionParser.InvertedListValueListContext invertedValueList = ctx.invertedListValueList();

        if (valueList != null || invertedValueList != null) {
            ConditionParser.ValueListIdentifierContext list =
                    valueList != null ? valueList.valueListIdentifier() : invertedValueList.valueListIdentifier();
            boolean isInverted = valueList == null;

            List<?> valueListObjects = list.valueIdentifier().stream()
                    .map(ParsingUtils::parseValueIdentifier)
                    .toList();

            return new ListValueList(isInverted, valueListObjects);
        }
        throw new DslParseException("Could not translate ListValueList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | ListReturnType.INVERTED;
        }

        return ListExp.getByValueList(cdtReturnType, Exp.val(valueList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
