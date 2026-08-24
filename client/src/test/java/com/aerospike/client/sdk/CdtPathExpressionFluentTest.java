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
 * WARRANTIES OR ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.path.CdtPathExpressionAel;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.LoopVarPart;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Unit tests for fluent CDT path context assembly and unsupported AEL path overloads (no server required).
 */
public class CdtPathExpressionFluentTest {

    @Test
    void finishPathRequiresEachChild() {
        CdtOperationParams p = new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get("book"));
        assertThrows(IllegalStateException.class, p::finishContextPathForPathExpression);
    }

    @Test
    void binRootEachChildThenMapKeyProducesTwoCtx() {
        CdtOperationParams p = CdtOperationParams.forEachChildAtBinRoot();
        p.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get("price"));
        CTX[] ctx = p.finishContextPathForPathExpression();
        assertEquals(2, ctx.length);
    }

    @Test
    void eachChildWithFilterPushesFilteredCtx() {
        CdtOperationParams p = new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get("book"));
        p.pushCurrentToContextAndReplaceWithAllChildrenWithFilter(Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(0)));
        p.pushCurrentToContextAndReplaceWith(CdtOperation.MAP_BY_KEY, Value.get("title"));
        CTX[] ctx = p.finishContextPathForPathExpression();
        assertEquals(3, ctx.length);
    }

    @Test
    void aelStubThrows() {
        assertThrows(UnsupportedOperationException.class, CdtPathExpressionAel::throwAelNotSupported);
        assertThrows(UnsupportedOperationException.class,
                () -> CdtPathExpressionAel.throwPreparedAelNotSupported(new PreparedAel("x == ?"), 1));
    }
}
