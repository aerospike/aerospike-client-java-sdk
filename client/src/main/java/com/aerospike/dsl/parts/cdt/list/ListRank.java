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
package com.aerospike.dsl.parts.cdt.list;

import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.exp.ListExp;
import com.aerospike.dsl.ConditionParser;
import com.aerospike.dsl.parts.path.BasePath;

public class ListRank extends ListPart {
    private final int rank;

    public ListRank(int rank) {
        super(ListPartType.RANK);
        this.rank = rank;
    }

    public static ListRank from(ConditionParser.ListRankContext ctx) {
        return new ListRank(Integer.parseInt(ctx.INT().getText()));
    }

    @Override
    public Exp constructExp(BasePath basePath, Exp.Type valueType, int cdtReturnType, CTX[] context) {
        return ListExp.getByRank(cdtReturnType, valueType, Exp.val(rank),
                Exp.bin(basePath.getBinPart().getBinName(), basePath.getBinType()), context);
    }

    @Override
    public CTX getContext() {
        return CTX.listRank(rank);
    }
}
