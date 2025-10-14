package com.aerospike.client.fluent.dsl;

/**
 * Example demonstrating the type-safe DSL.
 * This shows how the DSL provides compile-time type checking.
 */
public class Example {
    public static void main(String[] args) {
        // Create typed bin expressions
        LongBin ageBin = Dsl.longBin("age");
        StringBin nameBin = Dsl.stringBin("name");
        DoubleBin salaryBin = Dsl.doubleBin("salary");
        BooleanBin activeBin = Dsl.booleanBin("active");
        
        // Type-safe comparisons - these will compile
        BooleanExpression expr1 = ageBin.gt(18).and(nameBin.eq("Tim"));  // Using int instead of long
        BooleanExpression expr2 = salaryBin.gte(50000.0f).and(activeBin.eq(true));  // Using float instead of double
        ageBin.gt(17);  // Using int instead of long
        
        // Type-safe arithmetic - these will compile
        LongExpression agePlusOne = ageBin.add(1);  // Using int instead of long
        DoubleExpression salaryPlusBonus = salaryBin.add(10000.0f);  // Using float instead of double
        
        // Type conversion - these will compile
        DoubleExpression ageAsDouble = ageBin.toFloat();
        LongExpression salaryAsInt = salaryBin.toInt();
        
        // Complex expression with type safety
        BooleanExpression complexExpr = ageBin.gt(17)  // Using int instead of long
            .and(nameBin.eq("Tim"))
            .and(salaryBin.gte(50000.0f))  // Using float instead of double
            .and(activeBin.eq(true));
        
        System.out.println("Expression 1: " + expr1);
        System.out.println("Aerospike: " + expr1.toAerospikeExpr());
        
        System.out.println("\nExpression 2: " + expr2);
        System.out.println("Aerospike: " + expr2.toAerospikeExpr());
        
        System.out.println("\nComplex Expression: " + complexExpr);
        System.out.println("Aerospike: " + complexExpr.toAerospikeExpr());
        
        System.out.println("\nArithmetic: " + agePlusOne);
        System.out.println("Aerospike: " + agePlusOne.toAerospikeExpr());
        
        // Additional examples showing type flexibility
        System.out.println("\n--- Type Flexibility Examples ---");
        
        // Using int literals with Long expressions
        LongExpression agePlusTen = ageBin.add(10);  // int -> long
        BooleanExpression ageCheck = ageBin.gte(21);  // int -> long
        
        // Using float literals with Double expressions  
        DoubleExpression salaryWithBonus = salaryBin.add(5000.0f);  // float -> double
        BooleanExpression salaryCheck = salaryBin.lt(75000.0f);  // float -> double
        
        System.out.println("Age + 10: " + agePlusTen);
        System.out.println("Aerospike: Age + 10: " + agePlusTen.toAerospikeExpr());
        System.out.println("Age check: " + ageCheck);
        System.out.println("Aerospike: Age check: " + ageCheck.toAerospikeExpr());
        System.out.println("Salary + bonus: " + salaryWithBonus);
        System.out.println("Aerospike: Salary + bonus: " + salaryWithBonus.toAerospikeExpr());
        System.out.println("Salary check: " + salaryCheck);
        System.out.println("Aerospike: Salary check: " + salaryCheck.toAerospikeExpr());
        
        // Operator precedence examples
        System.out.println("\n--- Operator Precedence Examples ---");
        
        // Example 1: age + 3 * 4 - 2
        // Standard precedence: age + (3 * 4) - 2 = age + 12 - 2 = age + 10
        // With fluent API (left-to-right): ((age + 3) * 4) - 2 = (age + 3) * 4 - 2
        LongExpression precedence1 = ageBin.add(3).mul(4).sub(2);
        System.out.println("age + 3 * 4 - 2 (fluent API): " + precedence1);
        System.out.println("Aerospike: " + precedence1.toAerospikeExpr());
        
        // Example 2: age + 3 * (4 - 2)
        // Standard precedence: age + (3 * (4 - 2)) = age + (3 * 2) = age + 6
        // With fluent API: ((age + 3) * 4) - 2 (same as above)
        LongExpression precedence2 = ageBin.add(3).mul(4).sub(2);
        System.out.println("age + 3 * (4 - 2) (fluent API): " + precedence2);
        System.out.println("Aerospike: " + precedence2.toAerospikeExpr());
        
        // Example 3: (age + 5) * 2 - 3
        // Standard precedence: (age + 5) * 2 - 3
        // With fluent API: ((age + 5) * 2) - 3
        LongExpression precedence3 = ageBin.add(5).mul(2).sub(3);
        System.out.println("(age + 5) * 2 - 3 (fluent API): " + precedence3);
        System.out.println("Aerospike: " + precedence3.toAerospikeExpr());
        
        // Example 4: salary * 1.1 + 1000
        // Standard precedence: (salary * 1.1) + 1000
        // With fluent API: ((salary * 1.1) + 1000)
        DoubleExpression precedence4 = salaryBin.mul(1.1f).add(1000.0f);
        System.out.println("salary * 1.1 + 1000 (fluent API): " + precedence4);
        System.out.println("Aerospike: " + precedence4.toAerospikeExpr());
        
        System.out.println("\n--- Precedence Explanation ---");
        System.out.println("The fluent API evaluates left-to-right:");
        System.out.println("  age.add(3).mul(4).sub(2) = ((age + 3) * 4) - 2");
        System.out.println("  This is NOT standard mathematical precedence!");
        System.out.println("  Standard precedence would be: age + (3 * 4) - 2 = age + 10");
        System.out.println("  But fluent API gives: ((age + 3) * 4) - 2 = (age + 3) * 4 - 2");
        System.out.println("\nTo achieve standard mathematical precedence, you would need to:");
        System.out.println("  1. Use a different API design with explicit parentheses");
        System.out.println("  2. Or manually construct the expression with proper grouping");
        
        // Example showing how to achieve correct precedence with current system
        System.out.println("\n--- Achieving Correct Precedence ---");
        System.out.println("To achieve standard mathematical precedence with the current fluent API,");
        System.out.println("you would need to manually construct expressions using literals:");
        System.out.println("  ageBin.add(Bins.longLiteral(3).mul(4)).sub(2L) for age + (3 * 4) - 2");
        System.out.println("  ageBin.add(Bins.longLiteral(3).mul(Bins.longLiteral(4).sub(2L))) for age + 3 * (4 - 2)");
        System.out.println("This is more verbose but gives you control over precedence.");
        
        System.out.println(ageBin.add(Dsl.val(3).mul(Dsl.val(4).sub(2))));
        
        // IF-THEN-ELSE examples
        System.out.println("\n--- IF-THEN-ELSE Examples ---");
        
        // Simple IF-THEN-ELSE
        IfExpression simpleIf = Dsl.if_(ageBin.gt(18), Dsl.val("Adult"))
                                   .else_(Dsl.val("Minor"));
        System.out.println("Simple IF-THEN-ELSE: " + simpleIf);
        System.out.println("Aerospike: " + simpleIf.toAerospikeExpr());
        
        // IF-THEN-ELSE IF-ELSE
        IfExpression complexIf = Dsl.if_(ageBin.lt(13), Dsl.val("Child"))
                                    .elseIf(ageBin.lt(20), Dsl.val("Teenager"))
                                    .elseIf(ageBin.lt(65), Dsl.val("Adult"))
                                    .else_(Dsl.val("Senior"));
        System.out.println("Complex IF-THEN-ELSE IF-ELSE: " + complexIf);
        System.out.println("Aerospike: " + complexIf.toAerospikeExpr());
        
        // IF with arithmetic expressions
        IfExpression arithmeticIf = Dsl.if_(salaryBin.gt(100000.0), 
                                            salaryBin.mul(0.9)) // 10% tax for high earners
                                       .elseIf(salaryBin.gt(50000.0), 
                                              salaryBin.mul(0.85)) // 15% tax for medium earners
                                       .else_(salaryBin.mul(0.8)); // 20% tax for others
        System.out.println("IF with arithmetic: " + arithmeticIf);
        System.out.println("Aerospike: " + arithmeticIf.toAerospikeExpr());
        
        // IF with mixed types
        IfExpression mixedIf = Dsl.if_(ageBin.gt(18).and(activeBin.eq(true)), 
                                       Dsl.val("Active Adult"))
                                  .elseIf(ageBin.gt(18), 
                                         Dsl.val("Inactive Adult"))
                                  .else_(Dsl.val("Minor"));
        System.out.println("IF with mixed types: " + mixedIf);
        System.out.println("Aerospike: " + mixedIf.toAerospikeExpr());
        
        // IF expressions inside other expressions
        System.out.println("\n--- IF Inside Expressions ---");
        
        // IF inside arithmetic: $.bina + if(binb > 10, 20, 30)
        LongExpression ageWithBonus = ageBin.add(
            Dsl.if_(ageBin.gt(25), Dsl.val(10))
                .else_(Dsl.val(5))
        );
        System.out.println("Age with bonus: " + ageWithBonus);
        System.out.println("Aerospike: " + ageWithBonus.toAerospikeExpr());
        
        // IF inside arithmetic with complex conditions
        DoubleExpression salaryWithAdjustment = salaryBin.add(
            Dsl.if_(salaryBin.gt(100000.0), Dsl.val(5000.0))
                .elseIf(salaryBin.gt(50000.0), Dsl.val(2000.0))
                .else_(Dsl.val(1000.0))
        );
        System.out.println("Salary with adjustment: " + salaryWithAdjustment);
        System.out.println("Aerospike: " + salaryWithAdjustment.toAerospikeExpr());
        
        // IF inside comparison - using the convenience method
        BooleanExpression complexCondition = ageBin.gt(
            Dsl.ifLong(
                Dsl.if_(activeBin.eq(true), Dsl.val(18))
                    .else_(Dsl.val(21))
            )
        );
        System.out.println("Complex condition: " + complexCondition);
        System.out.println("Aerospike: " + complexCondition.toAerospikeExpr());
        
        // Nested IF expressions
        LongExpression nestedIf = ageBin.add(
            Dsl.if_(ageBin.gt(65), Dsl.val(15))
                .elseIf(ageBin.gt(50), 
                    Dsl.if_(salaryBin.gt(75000.0), Dsl.val(10))
                        .else_(Dsl.val(5)))
                .else_(Dsl.val(0))
        );
        System.out.println("Nested IF: " + nestedIf);
        System.out.println("Aerospike: " + nestedIf.toAerospikeExpr());
        
        // Local variable examples
        System.out.println("\n--- Local Variable Examples ---");
        
        // Simple local variable: age + 4 * (define("calc").as(date - 17).thenReturn(var("calc") * 2))
        NumericExpression localVarExample = ageBin.add(4).mul(
            Dsl.localVarLong(
                Dsl.define("calc").as(Dsl.longBin("date").sub(17))
                    .thenReturn(Dsl.varLong("calc").mul(2))
            )
        );
        System.out.println("Local variable example: " + localVarExample);
        System.out.println("Aerospike: " + localVarExample.toAerospikeExpr());
        
        // Multiple local variables
        NumericExpression multipleVars = ageBin.add(
            Dsl.localVarLong(
                Dsl.define("base").as(Dsl.longBin("salary").div(12))
                    .and("bonus").as(Dsl.longBin("performance").mul(100))
                    .thenReturn(Dsl.varLong("base").add(Dsl.varLong("bonus")))
            )
        );
        System.out.println("Multiple local variables: " + multipleVars);
        System.out.println("Aerospike: " + multipleVars.toAerospikeExpr());
        
        // Local variables with IF expressions
        NumericExpression localVarWithIf = ageBin.add(
                Dsl.define("threshold").as(Dsl.val(25))
                    .and("bonus").as(
                        Dsl.if_(ageBin.gt(Dsl.varLong("threshold")), Dsl.val(1000))
                            .else_(Dsl.val(500))
                    )
                    .thenReturn(Dsl.varLong("bonus"))
        );
        System.out.println("Local variables with IF: " + localVarWithIf);
        System.out.println("Aerospike: " + localVarWithIf.toAerospikeExpr());
        
        
        // The following would cause compile-time errors:
        // ageBin.eq("Tim");           // Error: cannot compare Long with String
        // nameBin.add(1);             // Error: String doesn't support arithmetic
        // ageBin.eq(true);            // Error: cannot compare Long with Boolean
        // salaryBin.eq(42);           // Error: cannot compare Double with Integer
    }
}
