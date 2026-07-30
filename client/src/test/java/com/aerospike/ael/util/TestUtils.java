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
package com.aerospike.ael.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.aerospike.ael.ExpressionContext;
import com.aerospike.ael.IndexContext;
import com.aerospike.ael.ParsedExpression;
import com.aerospike.ael.impl.AelParserImpl;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.Filter;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TestUtils {

    public static final String NAMESPACE = "test1";
    private static final AelParserImpl parser = new AelParserImpl();


    /**
     * Parses the given AEL path String into array of {@link CTX}.
     *
     * @param pathToCtx String input representing AEL path
     * @return The array of {@link CTX} or null
     */
    public static CTX[] parseCtx(String pathToCtx) {
        return parser.parseCTX(pathToCtx);
    }

    /**
     * Parses the given AEL path String and compares arrays of {@link CTX} using {@link CTX#toBase64(CTX[])} method.
     *
     * @param pathToCtx String input representing AEL path
     * @param expected  The array of {@link CTX} to be used for comparing
     */
    public static void parseCtxAndCompareAsBase64(String pathToCtx, CTX[] expected) {
        CTX[] actualCtx = parser.parseCTX(pathToCtx);
        assertEquals(expected == null ? null : CTX.toBase64(expected), actualCtx == null ? null : CTX.toBase64(actualCtx));
    }
}
