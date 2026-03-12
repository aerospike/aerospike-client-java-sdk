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
package com.aerospike.dsl.parts.operand;

import com.aerospike.dsl.parts.AbstractPart;

/**
 * A factory for creating different types of {@link AbstractPart} operands based on a given value.
 * <p>
 * This factory provides a static method to dynamically create concrete operand implementations
 * such as {@link StringOperand}, {@link BooleanOperand}, {@link FloatOperand}, and {@link IntOperand}
 * from various Java primitive and wrapper types. It centralizes the logic for type-specific object creation.
 *
 * @see StringOperand
 * @see BooleanOperand
 * @see FloatOperand
 * @see IntOperand
 */
public interface OperandFactory {

    /**
     * Creates a concrete {@link AbstractPart} operand based on the type of the provided value.
     * <p>
     * This method handles the creation of operands for common data types:
     * <ul>
     * <li>{@link String} to {@link StringOperand}.</li>
     * <li>{@link Boolean} to {@link BooleanOperand}.</li>
     * <li>{@link Float} or {@link Double} to {@link FloatOperand}.</li>
     * <li>{@link Integer} or {@link Long} to {@link IntOperand}.</li>
     * </ul>
     *
     * @param value The object to be converted into an operand. This cannot be {@code null}.
     * @return A new instance of an operand that extends {@link AbstractPart}.
     * @throws IllegalArgumentException      If the value provided is {@code null}.
     * @throws UnsupportedOperationException If the type of the value is not supported by the factory.
     */
    static AbstractPart createOperand(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Cannot create operand from null value");
        }

        if (value instanceof String) {
            return new StringOperand((String) value);
        } else if (value instanceof Boolean) {
            return new BooleanOperand((Boolean) value);
        } else if (value instanceof Float || value instanceof Double) {
            return new FloatOperand(((Number) value).doubleValue());
        } else if (value instanceof Integer || value instanceof Long) {
            return new IntOperand(((Number) value).longValue());
        } else {
            throw new UnsupportedOperationException(String.format("Cannot create operand from value of type %s, " +
                            "only String, boolean, float, double, long and integer values are currently supported",
                    value.getClass().getSimpleName()));
        }
    }
}

