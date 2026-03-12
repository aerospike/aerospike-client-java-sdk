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
package com.aerospike.dsl.parts.cdt;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.dsl.DslParseException;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.path.BasePath;
import com.aerospike.dsl.parts.path.PathFunction;

public abstract class CdtPart extends AbstractPart {

    protected CdtPart(PartType partType) {
        super(partType);
    }

    public abstract Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context);

    public CTX getContext() {
        // should print the subclass of the cdt type
        throw new DslParseException("Context is not supported for %s".formatted(this.getClass().getName()));
    }

    public abstract int getReturnType(PathFunction.ReturnParam returnParam);

    public static boolean isCdtPart(AbstractPart part) {
        return part.getPartType() == AbstractPart.PartType.LIST_PART
                || part.getPartType() == AbstractPart.PartType.MAP_PART;
    }
}
