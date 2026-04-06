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
package com.aerospike.client.sdk.dsl;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for creating expressions with local variable scope.
 * Provides an API for defining variables and their scope.
 */
public class LocalVariableBuilder {
    private final List<VariableDefinition> variables = new ArrayList<>();
    private final String currentVariableName;
    
    // Constructor for chaining
    LocalVariableBuilder(String variableName, List<VariableDefinition> existingVariables) {
        this.currentVariableName = variableName;
        this.variables.addAll(existingVariables);
    }

    public LocalVariableBuilder(String variableName) {
        this.currentVariableName = variableName;
    }

    /**
     * Defines the value for the current variable.
     */
    public LocalVariableBuilder as(DslExpression value) {
        variables.add(new VariableDefinition(currentVariableName, value));
        return this;
    }

    /**
     * Adds another variable definition and returns a builder for it.
     * This method is used to chain multiple variable definitions.
     */
    public LocalVariableBuilder and(String variableName) {
        // Create a new builder that will collect all variables
        return new LocalVariableBuilder(variableName, this.variables);
    }

    /**
     * Completes the variable definition and specifies the result expression.
     */
    public LocalVariableExpression thenReturn(DslExpression resultExpression) {
        return new LocalVariableExpression(variables, resultExpression);
    }

    /**
     * Completes the variable definition and specifies the result expression.
     * This method allows chaining from the 'as' method.
     */
    public LocalVariableExpression thenReturn(DslExpression resultExpression, LocalVariableBuilder... additionalBuilders) {
        List<VariableDefinition> allVariables = new ArrayList<>(variables);
        
        for (LocalVariableBuilder builder : additionalBuilders) {
            allVariables.addAll(builder.variables);
        }
        
        return new LocalVariableExpression(allVariables, resultExpression);
    }
} 