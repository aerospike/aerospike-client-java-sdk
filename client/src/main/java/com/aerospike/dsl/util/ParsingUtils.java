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
package com.aerospike.dsl.util;

import com.aerospike.dsl.DslParseException;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ParsingUtils {

    /**
     * Get the string inside the quotes.
     *
     * @param str String input
     * @return String inside the quotes
     */
    public static String unquote(String str) {
        if (str.length() > 2) {
            return str.substring(1, str.length() - 1);
        } else {
            throw new IllegalArgumentException("String %s must contain more than 2 characters".formatted(str));
        }
    }

    /**
     * @param a Integer, can be null
     * @param b Integer, non-null
     * @return a - b if a != null, otherwise null
     */
    public static Integer subtractNullable(Integer a, @NonNull Integer b) {
        return a == null ? null : a - b;
    }

    /**
     * Extracts the type string from a method name expected to start with "as" and end with "()".
     *
     * @param methodName The method name string
     * @return The extracted type string
     * @throws DslParseException if the method name is not in the correct format
     */
    public static String extractTypeFromMethod(String methodName) {
        if (methodName.startsWith("as") && methodName.endsWith("()")) {
            return methodName.substring(2, methodName.length() - 2);
        } else {
            throw new DslParseException("Invalid method name: %s".formatted(methodName));
        }
    }

    /**
     * Extracts the function name from a string that may include parameters in parentheses.
     *
     * @param text The input string containing the function name and potentially parameters
     * @return The extracted function name
     */
    public static String extractFunctionName(String text) {
        int startParen = text.indexOf('(');
        return (startParen != -1) ? text.substring(0, startParen) : text;
    }

    /**
     * Extracts an integer parameter from a string enclosed in parentheses.
     *
     * @param text The input string
     * @return The extracted integer parameter, or {@code null} if not found or invalid
     */
    public static Integer extractParameter(String text) {
        int startParen = text.indexOf('(');
        int endParen = text.indexOf(')');

        if (startParen != -1 && endParen != -1 && endParen > startParen + 1) {
            String numberStr = text.substring(startParen + 1, endParen);
            return Integer.parseInt(numberStr);
        }
        return null;
    }
}
