package com.aerospike.client.fluent.dsl;

/**
 * Factory class for creating typed bin expressions.
 */
public class Dsl {
    
    // Long bin factory methods
    public static LongBin longBin(String name) {
        return new LongBin(name);
    }
    
    public static LongLiteralExpression val(Long value) {
        return new LongLiteralExpression(value);
    }
    
    public static LongLiteralExpression val(Integer value) {
        return new LongLiteralExpression(value.longValue());
    }
    
    // Double bin factory methods
    public static DoubleBin doubleBin(String name) {
        return new DoubleBin(name);
    }
    
    public static DoubleLiteralExpression val(Double value) {
        return new DoubleLiteralExpression(value);
    }
    
    public static DoubleLiteralExpression val(Float value) {
        return new DoubleLiteralExpression(value.doubleValue());
    }
    
    // String bin factory methods
    public static StringBin stringBin(String name) {
        return new StringBin(name);
    }
    
    public static StringLiteralExpression val(String value) {
        return new StringLiteralExpression(value);
    }
    
    // Boolean bin factory methods
    public static BooleanBin booleanBin(String name) {
        return new BooleanBin(name);
    }
    
    public static BooleanLiteralExpression val(Boolean value) {
        return new BooleanLiteralExpression(value);
    }
    
    public static BooleanLiteralExpression val(boolean value) {
        return new BooleanLiteralExpression(Boolean.valueOf(value));
    }
    
    // Blob bin factory methods
    public static BlobBin blobBin(String name) {
        return new BlobBin(name);
    }
    
    public static BlobLiteralExpression val(byte[] value) {
        return new BlobLiteralExpression(value);
    }
    
    // Type conversion functions
    public static LongExpression toInt(DoubleExpression doubleExpr) {
        return new DoubleToLongExpression(doubleExpr);
    }
    
    public static DoubleExpression toFloat(LongExpression longExpr) {
        return new LongToDoubleExpression(longExpr);
    }
    
    // IF-THEN-ELSE functions
    public static IfBuilder if_(BooleanExpression condition, DslExpression result) {
        return IfBuilder.if_(condition, result);
    }
    
    /**
     * Creates a LongExpression from an IfExpression for use in arithmetic operations.
     * This enables expressions like: $.bina + if(binb > 10, 20, 30)
     */
    public static LongExpression ifLong(IfExpression ifExpr) {
        return new IfLongExpression(ifExpr);
    }
    
    /**
     * Creates a DoubleExpression from an IfExpression for use in arithmetic operations.
     * This enables expressions like: $.bina + if(binb > 10.0, 20.0, 30.0)
     */
    public static DoubleExpression ifDouble(IfExpression ifExpr) {
        return new IfDoubleExpression(ifExpr);
    }
    
    // Local variable functions
    /**
     * Creates a reference to a local variable.
     * Used within expressions that have local variable scope.
     */
    public static VariableExpression var(String variableName) {
        return new VariableExpression(variableName);
    }
    
    /**
     * Creates a LongExpression from a VariableExpression for use in arithmetic operations.
     */
    public static LongExpression varLong(String variableName) {
        return new VariableLongExpression(new VariableExpression(variableName));
    }
    
    /**
     * Starts defining a local variable.
     * Usage: Bins.define("varName").as(expression).thenReturn(resultExpression)
     */
    public static LocalVariableBuilder define(String variableName) {
        return new LocalVariableBuilder(variableName);
    }
    
    /**
     * Creates a LongExpression from a LocalVariableExpression for use in arithmetic operations.
     */
    public static LongExpression localVarLong(LocalVariableExpression localVarExpr) {
        return new LocalVariableLongExpression(localVarExpr);
    }
}
