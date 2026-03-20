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

import java.util.List;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.cdt.MapReturnType;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.MapExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.util.ParsingUtils;

public class MapKeyList extends MapPart {
    private final boolean isInverted;
    private final List<String> keyList;

    public MapKeyList(boolean isInverted, List<String> keyList) {
        super(MapPartType.KEY_LIST);
        this.isInverted = isInverted;
        this.keyList = keyList;
    }

    public static MapKeyList from(ConditionParser.MapKeyListContext ctx) {
        ConditionParser.StandardMapKeyListContext keyList = ctx.standardMapKeyList();
        ConditionParser.InvertedMapKeyListContext invertedKeyList = ctx.invertedMapKeyList();

        if (keyList != null || invertedKeyList != null) {
            ConditionParser.KeyListIdentifierContext list =
                    keyList != null ? keyList.keyListIdentifier() : invertedKeyList.keyListIdentifier();
            boolean isInverted = keyList == null;

            List<String> keyListStrings = list.mapKey().stream()
                    .map(ParsingUtils::parseMapKey)
                    .toList();

            return new MapKeyList(isInverted, keyListStrings);
        }
        throw new DslParseException("Could not translate MapKeyList from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        return MapExp.getByKeyList(cdtReturnType, Exp.val(keyList),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }
}
