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
package com.aerospike.client.sdk.junit;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public final class RequiresServerFeatureExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        List<ServerFeature> required = requiredFeatures(context);
        if (required.isEmpty()) {
            return ConditionEvaluationResult.enabled("no @RequiresServerFeature");
        }

        for (ServerFeature feature : required) {
            if (!feature.isSupported()) {
                return ConditionEvaluationResult.disabled(feature.skipMessage());
            }
        }
        return ConditionEvaluationResult.enabled("cluster supports " + required);
    }

    private static List<ServerFeature> requiredFeatures(ExtensionContext context) {
        List<ServerFeature> features = new ArrayList<>();

        context.getTestMethod()
            .flatMap(method -> AnnotationSupport.findAnnotation(method, RequiresServerFeature.class))
            .ifPresent(annotation -> features.add(annotation.value()));

        Class<?> type = context.getRequiredTestClass();
        while (type != null && type != Object.class) {
            Optional<RequiresServerFeature> onType =
                AnnotationSupport.findAnnotation(type, RequiresServerFeature.class);
            onType.ifPresent(annotation -> features.add(annotation.value()));
            type = type.getEnclosingClass();
        }

        return features;
    }
}
