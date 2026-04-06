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

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.dsl.parts.AbstractPart;
import com.aerospike.dsl.parts.cdt.map.MapTypeDesignator;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TypeUtils {

    /**
     * Returns the default {@link Exp.Type} for a given {@link AbstractPart}.
     * For {@link AbstractPart.PartType#MAP_PART} that is not a {@link MapTypeDesignator}, the default type is {@code STRING}.
     * Otherwise, the default type is {@code INT}.
     *
     * @param part The {@link AbstractPart} for which to determine the default type
     * @return The default {@link Exp.Type} for the given part
     */
    public static Exp.Type getDefaultType(AbstractPart part) {
        if (part.getPartType() == AbstractPart.PartType.MAP_PART
                // MapTypeDesignator is usually combined with int based operations such as size
                && !(part instanceof MapTypeDesignator)) {
            // For all other Map parts the default type should be STRING
            return Exp.Type.STRING;
        } else {
            // Default INT
            return Exp.Type.INT;
        }
    }

    // When return type is COUNT, always return INT as default type
    public static Exp.Type getDefaultTypeForCount() {
        return Exp.Type.INT;
    }
}
