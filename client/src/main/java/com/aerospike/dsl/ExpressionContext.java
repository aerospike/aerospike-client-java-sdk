package com.aerospike.dsl;

/**
 * This class stores input string and optional values for placeholders (if they are used)
 */
public class ExpressionContext {

    /**
     * Input string. If placeholders are used, they should be matched with {@code values}
     */
    private final String expression;
    /**
     * {@link PlaceholderValues} to be matched with placeholders in the {@code input} string.
     * Optional (needed only if there are placeholders)
     */
    private PlaceholderValues values;

    public ExpressionContext(String expression, PlaceholderValues values) {
    	this.expression = expression;
    	this.values = values;
    }

    public static ExpressionContext of(String expression) {
    	return new ExpressionContext(expression, null);
    }

    public String getExpression() {
    	return expression;
    }

    public PlaceholderValues getValues() {
    	return values;
    }
}
