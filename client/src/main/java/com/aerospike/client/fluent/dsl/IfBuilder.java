package com.aerospike.client.fluent.dsl;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for creating IF-THEN-ELSE IF-ELSE expressions.
 * Provides a fluent API for constructing conditional expressions.
 */
public class IfBuilder {
    private final List<BooleanExpression> conditions = new ArrayList<>();
    private final List<DslExpression> results = new ArrayList<>();
    private DslExpression elseResult;

    /**
     * Starts an IF expression with the first condition and result.
     */
    public static IfBuilder if_(BooleanExpression condition, DslExpression result) {
        IfBuilder builder = new IfBuilder();
        builder.conditions.add(condition);
        builder.results.add(result);
        return builder;
    }

    /**
     * Adds an ELSE IF clause.
     */
    public IfBuilder elseIf(BooleanExpression condition, DslExpression result) {
        conditions.add(condition);
        results.add(result);
        return this;
    }

    /**
     * Adds the ELSE clause and builds the final expression.
     */
    public IfExpression else_(DslExpression result) {
        this.elseResult = result;
        return new IfExpression(conditions, results, elseResult);
    }
} 