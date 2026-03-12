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
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.parts.cdt.CdtPart;
import com.aerospike.dsl.parts.path.BasePath;

/**
 * Designates that the element to the left is a Map.
 */
public class MapTypeDesignator extends MapPart {

    public MapTypeDesignator() {
        super(MapPartType.MAP_TYPE_DESIGNATOR);
    }

    public static MapTypeDesignator from() {
        return new MapTypeDesignator();
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        var partsUpToDesignator = basePath.getCdtParts().subList(0, basePath.getCdtParts().size() - 1);
        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        int partsUpToDesignatorSize = partsUpToDesignator.size();

        if ((partsUpToDesignatorSize > 0)) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignatorSize - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        }
        // only bin
        return Exp.mapBin(basePath.getBinPart().getBinName());
    }
}
