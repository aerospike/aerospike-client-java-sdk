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
package com.aerospike.dsl;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.Filter;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * This class stores result of parsing DSL expression using {@link ParsedExpression#getResult()}
 * in form of Java client's secondary index {@link Filter} and filter {@link Exp}.
 */
@AllArgsConstructor
@Getter
public class ParseResult {

    /**
     * Secondary index {@link Filter}. Can be null in case of invalid or unsupported DSL string
     */
    Filter filter;
    /**
     * Filter {@link Exp}. Can be null in case of invalid or unsupported DSL string
     */
    Exp exp;

    /**
     * Return compiled expression if exp exists. Otherwise, return null.
     */
    public final Expression getExpression() {
    	return (exp != null)? Exp.build(exp) : null;
    }
}
