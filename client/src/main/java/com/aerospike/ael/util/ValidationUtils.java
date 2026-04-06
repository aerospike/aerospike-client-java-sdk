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

import com.aerospike.ael.DslParseException;
import com.aerospike.client.sdk.exp.Exp;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ValidationUtils {

    /**
     * Validates if two {@link Exp.Type} instances are comparable.
     * Comparison is allowed if both types are the same, or if one is {@code INT} and the other is {@code FLOAT}.
     * If the types are not comparable, a {@link DslParseException} is thrown.
     *
     * @param leftType The {@link Exp.Type} of the left operand. Can be {@code null}
     * @param rightType The {@link Exp.Type} of the right operand. Can be {@code null}
     * @throws DslParseException If both types are not {@code null} and are not comparable
     */
    public static void validateComparableTypes(Exp.Type leftType, Exp.Type rightType) {
        if (leftType != null && rightType != null) {
            boolean isIntAndFloat =
                    (leftType.equals(Exp.Type.INT) && rightType.equals(Exp.Type.FLOAT)) ||
                            (leftType.equals(Exp.Type.FLOAT) && rightType.equals(Exp.Type.INT));

            if (!leftType.equals(rightType) && !isIntAndFloat) {
                throw new DslParseException("Cannot compare %s to %s".formatted(leftType, rightType));
            }
        }
    }
}
