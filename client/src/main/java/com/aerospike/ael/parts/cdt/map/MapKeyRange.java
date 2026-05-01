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

import java.util.Optional;

import com.aerospike.ael.AelParseException;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.ael.util.ParsingUtils;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.MapReturnType;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.MapExp;
import com.aerospike.ael.ConditionParser;

public class MapKeyRange extends MapPart {
    private final boolean isInverted;
    private final Object start;
    private final Object end;

    public MapKeyRange(boolean isInverted, Object start, Object end) {
        super(MapPartType.KEY_RANGE);
        requireSupportedKeyType(start, "MapKeyRange start");
        if (end != null) {
            requireSupportedKeyType(end, "MapKeyRange end");
        }
        this.isInverted = isInverted;
        this.start = start;
        this.end = end;
    }

    public static MapKeyRange from(ConditionParser.MapKeyRangeContext ctx) {
        ConditionParser.StandardMapKeyRangeContext keyRange = ctx.standardMapKeyRange();
        ConditionParser.InvertedMapKeyRangeContext invertedKeyRange = ctx.invertedMapKeyRange();

        if (keyRange != null || invertedKeyRange != null) {
            ConditionParser.KeyRangeIdentifierContext range =
                    keyRange != null ? keyRange.keyRangeIdentifier() : invertedKeyRange.keyRangeIdentifier();
            boolean isInverted = keyRange == null;

            Object startKey = ParsingUtils.parseMapKey(range.mapKey(0));

            Object endKey = Optional.ofNullable(range.mapKey(1))
                    .map(ParsingUtils::parseMapKey)
                    .orElse(null);

            return new MapKeyRange(isInverted, startKey, endKey);
        }
        throw new AelParseException("Could not translate MapKeyRange from ctx: %s".formatted(ctx));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        if (isInverted) {
            cdtReturnType = cdtReturnType | MapReturnType.INVERTED;
        }

        Exp startExp = ParsingUtils.objectToExp(start);
        Exp endExp = end != null ? ParsingUtils.objectToExp(end) : null;

        return MapExp.getByKeyRange(cdtReturnType, startExp, endExp, Exp.bin(basePath.getBinPart().getBinName(),
                basePath.getBinType()), context);
    }
}
