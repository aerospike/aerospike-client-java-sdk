package com.aerospike.client.fluent.dsl;

import java.util.List;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents an expression with local variable scope.
 * Defines one or more local variables and evaluates an expression in their scope.
 */
public class LocalVariableExpression implements DslExpression {
    private final List<VariableDefinition> variables;
    private final DslExpression resultExpression;

    public LocalVariableExpression(List<VariableDefinition> variables, DslExpression resultExpression) {
        this.variables = variables;
        this.resultExpression = resultExpression;
    }

    public List<VariableDefinition> getVariables() {
        return variables;
    }

    public DslExpression getResultExpression() {
        return resultExpression;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("define(");

        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(variables.get(i));
        }

        sb.append(").thenReturn(").append(resultExpression).append(")");
        return sb.toString();
    }

    @Override
    public String toAerospikeExpr() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exp.let(");

        for (int i = 0; i < variables.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(variables.get(i).toAerospikeExpr());
        }

        sb.append(", ").append(resultExpression.toAerospikeExpr()).append(")");
        return sb.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        Exp[] exps = new Exp[variables.size() + 1];
        for (int i = 0; i < variables.size(); i++) {
            exps[i] = Exp.def(variables.get(i).getName(), variables.get(i).getValue().toAerospikeExp());
        }
        exps[variables.size()] = resultExpression.toAerospikeExp();
        return Exp.let(exps);
    }
}