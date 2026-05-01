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
package com.aerospike.ael.parts.path;

import static com.aerospike.ael.parts.cdt.CdtPart.isCdtPart;
import static com.aerospike.ael.util.PathOperandUtils.getContextArray;
import static com.aerospike.ael.util.PathOperandUtils.processGet;
import static com.aerospike.ael.util.PathOperandUtils.processPathFunction;
import static com.aerospike.ael.util.PathOperandUtils.processSize;
import static com.aerospike.ael.util.PathOperandUtils.processValueType;
import static com.aerospike.ael.util.PathOperandUtils.updateWithCdtTypeDesignator;

import java.util.List;

import com.aerospike.ael.parts.AbstractPart;
import com.aerospike.ael.parts.cdt.CdtPart;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;

import lombok.Getter;

@Getter
public class Path extends AbstractPart {

    private final BasePath basePath;
    private final PathFunction pathFunction;

    public Path(BasePath basePath, PathFunction pathFunction) {
        super(PartType.PATH_OPERAND);
        this.basePath = basePath;
        this.pathFunction = pathFunction;
    }

    public Exp processPath(BasePath basePath, PathFunction pathFunction) {
        List<AbstractPart> parts = basePath.getCdtParts();
        updateWithCdtTypeDesignator(basePath, pathFunction);
        AbstractPart lastPathPart = !parts.isEmpty() ? parts.get(parts.size() - 1) : null;
        pathFunction = processPathFunction(basePath, lastPathPart, pathFunction);
        Exp.Type valueType = processValueType(lastPathPart, pathFunction);

        int cdtReturnType = 0;
        if (lastPathPart != null && isCdtPart(lastPathPart)) {
            cdtReturnType = ((CdtPart) lastPathPart).getReturnType(pathFunction.getReturnParam());
        }

        if (lastPathPart != null) {
            Exp exp = switch (pathFunction.getPathFunctionType()) {
                case GET, COUNT, CAST, EXISTS -> processGet(basePath, lastPathPart, valueType, cdtReturnType);
                case SIZE -> processSize(basePath, lastPathPart, valueType, cdtReturnType);
            };
            if (pathFunction.getPathFunctionType() == PathFunction.PathFunctionType.CAST && exp != null) {
                exp = pathFunction.getBinType() == Exp.Type.FLOAT ? Exp.toFloat(exp) : Exp.toInt(exp);
            }
            return exp;
        }
        return null;
    }

    @Override
    public Exp getExp() {
        return processPath(basePath, pathFunction);
    }

    @Override
    public CTX[] getCtx() {
        return getContextArray(basePath.getCdtParts(), true);
    }
}
