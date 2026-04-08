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

import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.PathFunction;
import com.aerospike.client.sdk.cdt.MapReturnType;

import lombok.Getter;

@Getter
public abstract class MapPart extends CdtPart {

    private final MapPartType mapPartType;

    protected MapPart(MapPartType mapPartType) {
        super(PartType.MAP_PART);
        this.mapPartType = mapPartType;
    }

    @Override
    public int getReturnType(PathFunction.ReturnParam returnParam) {
        return switch (returnParam) {
            case VALUE -> MapReturnType.VALUE;
            case KEY_VALUE -> MapReturnType.KEY_VALUE;
            case UNORDERED_MAP -> MapReturnType.UNORDERED_MAP;
            case ORDERED_MAP -> MapReturnType.ORDERED_MAP;
            case KEY -> MapReturnType.KEY;
            case INDEX -> MapReturnType.INDEX;
            case RANK -> MapReturnType.RANK;
            case COUNT, NONE -> MapReturnType.COUNT;
            case EXISTS -> MapReturnType.EXISTS;
            case REVERSE_INDEX -> MapReturnType.REVERSE_INDEX;
            case REVERSE_RANK -> MapReturnType.REVERSE_RANK;
        };
    }

    public enum MapPartType {
        MAP_TYPE_DESIGNATOR,
        KEY,
        INDEX,
        VALUE,
        RANK,
        KEY_RANGE,
        KEY_LIST,
        INDEX_RANGE,
        VALUE_LIST,
        VALUE_RANGE,
        RANK_RANGE,
        RANK_RANGE_RELATIVE,
        INDEX_RANGE_RELATIVE
    }
}
