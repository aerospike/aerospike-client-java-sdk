package com.aerospike.client.fluent.dsl.stub;

/**
 * TEMPORARY STUB - Replace with com.aerospike.dsl.impl.DSLParserImpl when library is fixed
 * 
 * Stub implementation of DSL parser
 */
public class DSLParserImpl implements DSLParser {
    @Override
    public ParsedExpression parse(String dslString, IndexContext indexContext) {
        // Minimal implementation - parse string into expression
        // For now, just return a simple ParsedExpression wrapper
        throw new UnsupportedOperationException(
            "DSL string parsing not yet implemented in stub. " +
            "Use BooleanExpression objects or PreparedDsl instead."
        );
    }
    
    @Override
    public ParsedExpression parseExpression(ExpressionContext context, IndexContext indexContext) {
        // Parse with index context
        return context.parse();
    }
    
    @Override
    public ParsedExpression parseExpression(ExpressionContext context) {
        // Parse without index context
        return context.parse();
    }
}

