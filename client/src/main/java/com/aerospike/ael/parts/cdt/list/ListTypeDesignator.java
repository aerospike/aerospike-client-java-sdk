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

import java.util.Collections;
import java.util.List;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;

/**
 * Designates that element to the left is a List.
 */
public class ListTypeDesignator extends ListPart {

    public ListTypeDesignator() {
        super(ListPartType.LIST_TYPE_DESIGNATOR);
    }

    public static ListTypeDesignator from() {
        return new ListTypeDesignator();
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        List<AbstractPart> partsUpToDesignator = !basePath.getCdtParts().isEmpty()
                ? basePath.getCdtParts().subList(0, basePath.getCdtParts().size() - 1)
                : Collections.emptyList();

        BasePath basePathUntilDesignator = new BasePath(basePath.getBinPart(), partsUpToDesignator);
        if (!partsUpToDesignator.isEmpty()) {
            return ((CdtPart) partsUpToDesignator.get(partsUpToDesignator.size() - 1))
                    .constructExp(basePathUntilDesignator, valueType, cdtReturnType, context);
        }
        // only bin
        return Exp.listBin(basePath.getBinPart().getBinName());
    }
}
