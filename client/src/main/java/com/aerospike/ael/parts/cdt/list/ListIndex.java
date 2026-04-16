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

import static com.aerospike.ael.util.ParsingUtils.parseSignedInt;

import com.aerospike.ael.parts.path.BasePath;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ListExp;

public class ListIndex extends ListPart {
    private final int index;

    public ListIndex(int index) {
        super(ListPartType.INDEX);
        this.index = index;
    }

    public static ListIndex from(ConditionParser.ListIndexContext ctx) {
        return new ListIndex(parseSignedInt(ctx.signedInt()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByIndex(cdtReturnType, valueType, Exp.val(index),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.listIndex(index);
    }
}
