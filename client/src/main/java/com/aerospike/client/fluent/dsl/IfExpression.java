package com.aerospike.client.fluent.dsl;

import java.util.List;

import com.aerospike.client.fluent.exp.Exp;

/**
 * Represents an IF-THEN-ELSE IF-ELSE expression.
 * Takes an odd number of arguments: boolean expressions alternating with result expressions.
 * The last expression is the ELSE clause.
 */
public class IfExpression implements DslExpression {
    private final List<BooleanExpression> conditions;
    private final List<DslExpression> results;
    private final DslExpression elseResult;

    public IfExpression(List<BooleanExpression> conditions, List<DslExpression> results, DslExpression elseResult) {
        this.conditions = conditions;
        this.results = results;
        this.elseResult = elseResult;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IF ");

        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                sb.append(" ELSE IF ");
            }
            sb.append(conditions.get(i)).append(" THEN ").append(results.get(i));
        }

        if (elseResult != null) {
            sb.append(" ELSE ").append(elseResult);
        }

        return sb.toString();
    }

    @Override
    public String toAerospikeExpr() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exp.cond(");

        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(conditions.get(i).toAerospikeExpr())
              .append(", ")
              .append(results.get(i).toAerospikeExpr());
        }

        if (elseResult != null) {
            sb.append(", ").append(elseResult.toAerospikeExpr());
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public Exp toAerospikeExp() {
        Exp[] conds = new Exp[conditions.size() * 2 + 1];
        for (int i = 0; i < conditions.size(); i++) {
            conds[i*2] = conditions.get(i).toAerospikeExp();
            conds[i*2+1] = results.get(i).toAerospikeExp();
        }
        conds[conditions.size()*2] = elseResult.toAerospikeExp();
        return Exp.cond(conds);
    }
}