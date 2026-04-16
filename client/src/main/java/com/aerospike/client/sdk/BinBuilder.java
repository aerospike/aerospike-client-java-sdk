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
package com.aerospike.client.sdk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Consumer;

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.exp.ExpWriteFlags;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Operations for one bin: scalar writes ({@link #setTo}), reads ({@link #get}), string ops, numeric {@link #add},
 * expression-backed {@link #selectFrom}, {@link #insertFrom}, {@link #updateFrom}, {@link #upsertFrom}, and nested
 * list/map CDT paths via {@code onMap*} / {@code onList*} (see {@link AbstractCdtBuilder} for list/map commands once
 * context is selected).
 *
 * @param <T> concrete operation builder type for chaining
 */
public class BinBuilder<T extends AbstractOperationBuilder<T>> extends AbstractCdtBuilder<T> {

    /**
     * @param opBuilder parent builder that collects bins and operations
     * @param binName   target bin name
     */
    public BinBuilder(T opBuilder, String binName) {
        super(opBuilder, binName, null);
    }

    /**
     * Queues a write that sets this bin to a string value.
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(String value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to an integer (stored as a numeric bin).
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(int value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a long (stored as a numeric bin).
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(long value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a float (stored as a double on the server).
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(float value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a double.
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(double value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a boolean.
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(boolean value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a blob (bytes).
     *
     * @param value value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(byte[] value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a list, preserving {@link AerospikeList} order metadata when packed.
     *
     * @param value list to store
     * @return the parent operation builder for chaining
     */
    public T setTo(AerospikeList<?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a list (default list encoding).
     *
     * @param value list to store
     * @return the parent operation builder for chaining
     */
    public T setTo(List<?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a map, preserving {@link AerospikeMap} type metadata when packed.
     *
     * @param value map to store
     * @return the parent operation builder for chaining
     */
    public T setTo(AerospikeMap<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a map (default map encoding).
     *
     * @param value map to store
     * @return the parent operation builder for chaining
     */
    public T setTo(Map<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a sorted map (key-ordered map encoding).
     *
     * @param value map to store
     * @return the parent operation builder for chaining
     */
    public T setTo(SortedMap<?, ?> value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that removes this bin from the record (null bin).
     *
     * @return the parent operation builder for chaining
     */
    public T remove() {
        return opBuilder.setTo(Bin.asNull(binName));
    }

    /**
     * Queues a read of this bin in the parent operation (e.g. get or query projection).
     *
     * @return the parent operation builder for chaining
     */
    public T get() {
        return opBuilder.get(binName);
    }

    /**
     * Queues a string append on this bin (bin must hold a string).
     *
     * @param fragment text to append
     * @return the parent operation builder for chaining
     */
    public T append(String fragment) {
        return opBuilder.append(new Bin(binName, fragment));
    }

    /**
     * Queues a string prepend on this bin (bin must hold a string).
     *
     * @param fragment text to prepend
     * @return the parent operation builder for chaining
     */
    public T prepend(String fragment) {
        return opBuilder.prepend(new Bin(binName, fragment));
    }

    /**
     * Queues a numeric add on this bin. If the record or bin is missing, it is created with {@code amount} as the value.
     *
     * @param amount delta to add
     * @return the parent operation builder for chaining
     */
    public T add(int amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    /**
     * Queues a numeric add on this bin. If the record or bin is missing, it is created with {@code amount} as the value.
     *
     * @param amount delta to add
     * @return the parent operation builder for chaining
     */
    public T add(long amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    /**
     * Queues a numeric add on this bin. If the record or bin is missing, it is created with {@code amount} as the value.
     *
     * @param amount delta to add
     * @return the parent operation builder for chaining
     */
    public T add(float amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    /**
     * Queues a numeric add on this bin. If the record or bin is missing, it is created with {@code amount} as the value.
     *
     * @param amount delta to add
     * @return the parent operation builder for chaining
     */
    public T add(double amount) {
        return opBuilder.add(new Bin(binName, amount));
    }

    // ==================================================================
    // Expression Operations - Read and write values computed from AEL expressions
    //
    // Each operation supports 5 AEL input types:
    // 1. String ael - AEL string expression
    // 2. BooleanExpression - Programmatic boolean expression
    // 3. PreparedAel - Prepared AEL with parameters
    // 4. Exp - Low-level expression builder
    // 5. Expression - Compiled expression
    //
    // Each also has an overload with Consumer<Options> for configuring flags.
    // ==================================================================

    // ----------------------------------------
    // selectFrom - Read expression operations
    // ----------------------------------------

    /**
     * Read a computed value from this bin using a AEL expression.
     * The result appears as a virtual bin in the returned record.
     *
     * <pre>{@code
     * // Compute total from price and quantity
     * session.query(key)
     *     .bin("total").selectFrom("$.price * $.quantity")
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @return the parent operation builder for chaining
     * @see #selectFrom(String, Consumer) for options like ignoreEvalFailure()
     */
    public T selectFrom(String ael) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
    }

    /**
     * Read a computed value with options.
     *
     * <pre>{@code
     * // Ignore errors if expression can't evaluate
     * session.query(key)
     *     .bin("ratio").selectFrom("$.a / $.b", opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @param options configure via {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T selectFrom(String ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
    }

    /**
     * Read a computed value using a programmatic BooleanExpression.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("isAdult").selectFrom(Exp.ge(Exp.intBin("age"), Exp.val(18)))
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T selectFrom(BooleanExpression ael) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT));
    }

    /**
     * Read a computed value using a BooleanExpression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("isAdult").selectFrom(myBoolExpr, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T selectFrom(BooleanExpression ael, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, opts.getFlags()));
    }

    /**
     * Read a computed value using a PreparedAel with bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.query(key)
     *     .bin("total").selectFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T selectFrom(PreparedAel ael, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, params, ExpReadFlags.DEFAULT));
    }

    /**
     * Read a computed value using a PreparedAel with options and bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.a / ?");
     * session.query(key)
     *     .bin("ratio").selectFrom(calc, opt -> opt.ignoreEvalFailure(), divisor)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param options configure via {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T selectFrom(PreparedAel ael, Consumer<ExpressionReadOptions> options, Object... params) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, params, opts.getFlags()));
    }

    /**
     * Read a computed value using a low-level Exp expression.
     *
     * <pre>{@code
     * Exp computation = Exp.mul(Exp.intBin("price"), Exp.intBin("quantity"));
     * session.query(key)
     *     .bin("total").selectFrom(computation)
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
    }

    /**
     * Read a computed value using a low-level Exp expression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("ratio").selectFrom(myExp, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Exp exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
    }

    /**
     * Read a computed value using a pre-compiled Expression.
     *
     * <pre>{@code
     * Expression compiled = Exp.build(Exp.mul(Exp.intBin("price"), Exp.intBin("qty")));
     * session.query(key)
     *     .bin("total").selectFrom(compiled)
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, ExpReadFlags.DEFAULT));
    }

    /**
     * Read a computed value using a pre-compiled Expression with options.
     *
     * <pre>{@code
     * session.query(key)
     *     .bin("ratio").selectFrom(compiledExp, opt -> opt.ignoreEvalFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @param options configure via {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Expression exp, Consumer<ExpressionReadOptions> options) {
        ExpressionReadOptions opts = new ExpressionReadOptions();
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // insertFrom - Write with CREATE_ONLY
    // ----------------------------------------

    /**
     * Write expression result only if the bin does not exist.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * // Set "total" only if it doesn't exist
     * session.upsert(key)
     *     .bin("total").insertFrom("$.price * $.quantity")
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @return the parent operation builder for chaining
     * @see #insertFrom(String, Consumer) to suppress failure if bin exists
     */
    public T insertFrom(String ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.CREATE_ONLY));
    }

    /**
     * Write expression result only if the bin does not exist, with options.
     *
     * <pre>{@code
     * // Set "total" only if it doesn't exist; don't fail if it does
     * session.upsert(key)
     *     .bin("total").insertFrom("$.price * $.quantity", opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T insertFrom(String ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a BooleanExpression.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").insertFrom(myBoolExpr)
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T insertFrom(BooleanExpression ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.CREATE_ONLY));
    }

    /**
     * Write expression result only if bin doesn't exist, using a BooleanExpression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").insertFrom(myBoolExpr, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T insertFrom(BooleanExpression ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a PreparedAel.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").insertFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T insertFrom(PreparedAel ael, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.CREATE_ONLY));
    }

    /**
     * Write expression result only if bin doesn't exist, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").insertFrom(calc, opt -> opt.ignoreOpFailure(), quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T insertFrom(PreparedAel ael, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, opts.getFlags()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a low-level Exp.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * Exp computation = Exp.mul(Exp.intBin("price"), Exp.intBin("quantity"));
     * session.upsert(key)
     *     .bin("total").insertFrom(computation)
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.CREATE_ONLY));
    }

    /**
     * Write expression result only if bin doesn't exist, using a low-level Exp with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").insertFrom(myExp, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a pre-compiled Expression.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * Expression compiled = Exp.build(Exp.mul(Exp.intBin("price"), Exp.intBin("qty")));
     * session.upsert(key)
     *     .bin("total").insertFrom(compiled)
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.CREATE_ONLY));
    }

    /**
     * Write expression result only if bin doesn't exist, using a pre-compiled Expression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").insertFrom(compiledExp, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // updateFrom - Write with UPDATE_ONLY
    // ----------------------------------------

    /**
     * Write expression result only if the bin already exists.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * // Update "total" only if it exists
     * session.upsert(key)
     *     .bin("total").updateFrom("$.price * $.quantity")
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @return the parent operation builder for chaining
     * @see #updateFrom(String, Consumer) to suppress failure if bin is missing
     */
    public T updateFrom(String ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.UPDATE_ONLY));
    }

    /**
     * Write expression result only if the bin already exists, with options.
     *
     * <pre>{@code
     * // Update "total" only if it exists; don't fail if it's missing
     * session.upsert(key)
     *     .bin("total").updateFrom("$.price * $.quantity", opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T updateFrom(String ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result only if bin already exists, using a BooleanExpression.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").updateFrom(myBoolExpr)
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T updateFrom(BooleanExpression ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.UPDATE_ONLY));
    }

    /**
     * Write expression result only if bin already exists, using a BooleanExpression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").updateFrom(myBoolExpr, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T updateFrom(BooleanExpression ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result only if bin already exists, using a PreparedAel.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").updateFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T updateFrom(PreparedAel ael, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.UPDATE_ONLY));
    }

    /**
     * Write expression result only if bin already exists, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").updateFrom(calc, opt -> opt.ignoreOpFailure(), quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T updateFrom(PreparedAel ael, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, opts.getFlags()));
    }

    /**
     * Write expression result only if bin already exists, using a low-level Exp.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * Exp computation = Exp.mul(Exp.intBin("price"), Exp.intBin("quantity"));
     * session.upsert(key)
     *     .bin("total").updateFrom(computation)
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.UPDATE_ONLY));
    }

    /**
     * Write expression result only if bin already exists, using a low-level Exp with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").updateFrom(myExp, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /**
     * Write expression result only if bin already exists, using a pre-compiled Expression.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * Expression compiled = Exp.build(Exp.mul(Exp.intBin("price"), Exp.intBin("qty")));
     * session.upsert(key)
     *     .bin("total").updateFrom(compiled)
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.UPDATE_ONLY));
    }

    /**
     * Write expression result only if bin already exists, using a pre-compiled Expression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").updateFrom(compiledExp, opt -> opt.ignoreOpFailure())
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @param options configure via {@code ignoreOpFailure()}, {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ----------------------------------------
    // upsertFrom - Write with DEFAULT (upsert)
    // ----------------------------------------

    /**
     * Write expression result, creating or overwriting the bin as needed.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * // Compute and store total (creates or overwrites)
     * session.upsert(key)
     *     .bin("total").upsertFrom("$.price * $.quantity")
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @return the parent operation builder for chaining
     * @see #upsertFrom(String, Consumer) for options like deleteIfNull()
     */
    public T upsertFrom(String ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.DEFAULT));
    }

    /**
     * Write expression result with options.
     *
     * <pre>{@code
     * // Delete bin if expression returns null
     * session.upsert(key)
     *     .bin("discount").upsertFrom("$.coupon", opt -> opt.deleteIfNull())
     *     .execute();
     * }</pre>
     *
     * @param ael the AEL expression string
     * @param options configure via {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(String ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result, creating or updating the bin, using a BooleanExpression.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").upsertFrom(myBoolExpr)
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(BooleanExpression ael) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.DEFAULT));
    }

    /**
     * Write expression result, creating or updating the bin, using a BooleanExpression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("flag").upsertFrom(myBoolExpr, opt -> opt.deleteIfNull())
     *     .execute();
     * }</pre>
     *
     * @param ael the boolean expression to evaluate
     * @param options configure via {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(BooleanExpression ael, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, opts.getFlags()));
    }

    /**
     * Write expression result, creating or updating the bin, using a PreparedAel.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").upsertFrom(calc, quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(PreparedAel ael, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.DEFAULT));
    }

    /**
     * Write expression result, creating or updating the bin, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?");
     * session.upsert(key)
     *     .bin("total").upsertFrom(calc, opt -> opt.deleteIfNull(), quantity)
     *     .execute();
     * }</pre>
     *
     * @param ael the prepared AEL statement
     * @param options configure via {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @param params parameter values to bind
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(PreparedAel ael, Consumer<ExpressionWriteOptions> options, Object... params) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, opts.getFlags()));
    }

    /**
     * Write expression result, creating or updating the bin, using a low-level Exp.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * Exp computation = Exp.mul(Exp.intBin("price"), Exp.intBin("quantity"));
     * session.upsert(key)
     *     .bin("total").upsertFrom(computation)
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Exp exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.DEFAULT));
    }

    /**
     * Write expression result, creating or updating the bin, using a low-level Exp with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").upsertFrom(myExp, opt -> opt.deleteIfNull())
     *     .execute();
     * }</pre>
     *
     * @param exp the Exp expression to evaluate
     * @param options configure via {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Exp exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    /**
     * Write expression result, creating or updating the bin, using a pre-compiled Expression.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * Expression compiled = Exp.build(Exp.mul(Exp.intBin("price"), Exp.intBin("qty")));
     * session.upsert(key)
     *     .bin("total").upsertFrom(compiled)
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Expression exp) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, ExpWriteFlags.DEFAULT));
    }

    /**
     * Write expression result, creating or updating the bin, using a pre-compiled Expression with options.
     *
     * <pre>{@code
     * session.upsert(key)
     *     .bin("total").upsertFrom(compiledExp, opt -> opt.deleteIfNull())
     *     .execute();
     * }</pre>
     *
     * @param exp the compiled expression to evaluate
     * @param options configure via {@code deleteIfNull()}, {@code ignoreEvalFailure()}
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Expression exp, Consumer<ExpressionWriteOptions> options) {
        ExpressionWriteOptions opts = new ExpressionWriteOptions(ExpWriteFlags.DEFAULT);
        options.accept(opts);
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, opts.getFlags()));
    }

    // ==================================================================
    // CDT Operations. Note: make sure to mirror these operations to
    // CdtContextNonInvertableBuilder and CdtContextInvertableBuilder
    // ==================================================================

    /**
     * Select a map entry by sort index for nested CDT ops on this bin's map.
     *
     * @param index index in server map order
     * @return builder for nested map CDT operations at that index
     */
    public CdtContextNonInvertableBuilder<T> onMapIndex(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX, index));
    }

    /**
     * Select a map entry by key.
     *
     * @param key map key
     * @return builder for nested map CDT operations at that key
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * Select a map entry by key, using {@code createType} if the map must be created.
     *
     * @param key        map key
     * @param createType ordering for a newly created map
     * @return builder for nested map CDT operations at that key
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(long key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }

    /**
     * @param key map key
     * @return builder for nested map CDT operations at that key
     * @see #onMapKey(long)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * @param key        map key
     * @param createType ordering for a newly created map
     * @return builder for nested map CDT operations at that key
     * @see #onMapKey(long, MapOrder)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(String key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }

    /**
     * @param key map key
     * @return builder for nested map CDT operations at that key
     * @see #onMapKey(long)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key)));
    }

    /**
     * @param key        map key
     * @param createType ordering for a newly created map
     * @return builder for nested map CDT operations at that key
     * @see #onMapKey(long, MapOrder)
     */
    public CdtSetterNonInvertableBuilder<T> onMapKey(byte[] key, MapOrder createType) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY, Value.get(key), createType));
    }

    /**
     * Select a map entry by rank.
     *
     * @param index rank in the map
     * @return builder for nested map CDT operations at that rank
     */
    public CdtContextNonInvertableBuilder<T> onMapRank(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK, index));
    }

    /**
     * Select map entries by value (identity match).
     *
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     */
    public CdtContextInvertableBuilder<T> onMapValue(long value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(String value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(byte[] value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * Restrict to map keys in {@code [startIncl, endExcl)}.
     *
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Restrict to map values in {@code [startIncl, endExcl)} (ordered comparison).
     *
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * Navigate to map items by key relative to index range.
     * Server selects map items nearest to key and greater by index.
     *
     * @param key the reference key
     * @param index the relative index offset
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }

    /**
     * @param key   the reference key
     * @param index the relative index offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }

    /**
     * @param key   the reference key
     * @param index the relative index offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapKeyRelativeIndexRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index));
    }

    /**
     * Navigate to map items by key relative to index range with count limit.
     * Server selects map items nearest to key and greater by index with a count limit.
     *
     * @param key the reference key
     * @param index the relative index offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(long key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }

    /**
     * @param key   the reference key
     * @param index the relative index offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapKeyRelativeIndexRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(String key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }

    /**
     * @param key   the reference key
     * @param index the relative index offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapKeyRelativeIndexRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRelativeIndexRange(byte[] key, int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_REL_INDEX_RANGE, Value.get(key), index, count));
    }

    /**
     * Navigate to map items by value relative to rank range.
     * Server selects map items nearest to value and greater by relative rank.
     *
     * @param value the reference value
     * @param rank the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank));
    }

    /**
     * Navigate to map items by value relative to rank range with count limit.
     * Server selects map items nearest to value and greater by relative rank with a count limit.
     *
     * @param value the reference value
     * @param rank the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(long value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(String value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(byte[] value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(double value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(boolean value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(List<?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value the reference value
     * @param rank  the relative rank offset
     * @param count the maximum number of items to select
     * @return builder for continued chaining (invertable for range operations)
     * @see #onMapValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onMapValueRelativeRankRange(SpecialValue value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count));
    }

    /**
     * Navigate to map items by index range.
     * Server selects "count" map items starting at specified index.
     *
     * @param index the starting index
     * @param count the number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index, count));
    }

    /**
     * Navigate to map items by index range to end.
     * Server selects map items starting at specified index to the end of map.
     *
     * @param index the starting index
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapIndexRange(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_INDEX_RANGE, index));
    }

    /**
     * Navigate to map items by rank range.
     * Server selects "count" map items starting at specified rank.
     *
     * @param rank the starting rank
     * @param count the number of items to select
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank, count));
    }

    /**
     * Navigate to map items by rank range to end.
     * Server selects map items starting at specified rank to the end of map.
     *
     * @param rank the starting rank
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onMapRankRange(int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_RANK_RANGE, rank));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(double value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(boolean value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(List<?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(Map<?,?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value value to locate in the map
     * @return builder for nested map CDT operations on matching entries
     * @see #onMapValue(long)
     */
    public CdtContextInvertableBuilder<T> onMapValue(SpecialValue value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE, value.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }
    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(SpecialValue startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(long startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(String startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start key
     * @param endExcl   exclusive end key
     * @return builder for map CDT operations on that key range
     * @see #onMapKeyRange(String, String)
     */
    public CdtActionInvertableBuilder<T> onMapKeyRange(double startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_KEY_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(long startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(String startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(double startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(boolean startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(List<?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start value
     * @param endExcl   exclusive end value
     * @return builder for map CDT operations on that value range
     * @see #onMapValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onMapValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.MAP_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * Select a list element by index for nested CDT ops on this bin's list.
     *
     * @param index list index
     * @return builder for nested list CDT operations at that index
     */
    public CdtContextNonInvertableBuilder<T> onListIndex(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index));
    }

    /**
     * Like {@link #onListIndex(int)} but supplies list creation policy when the list is created at this index.
     *
     * @param index list index
     * @param order list order if created
     * @param pad   whether to pad when creating
     * @return builder for nested list CDT operations at that index
     */
    public CdtContextNonInvertableBuilder<T> onListIndex(int index, ListOrder order, boolean pad) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX, index, order, pad));
    }

    /**
     * Select a list element by rank.
     *
     * @param index rank in the list
     * @return builder for nested list CDT operations at that rank
     */
    public CdtContextNonInvertableBuilder<T> onListRank(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_RANK, index));
    }

    /**
     * Select list elements matching a value.
     *
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     */
    public CdtContextInvertableBuilder<T> onListValue(long value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(String value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(byte[] value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(SpecialValue value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, value.toAerospikeValue()));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(double value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(boolean value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(List<?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * @param value element value to locate
     * @return builder for nested list CDT operations on matching elements
     * @see #onListValue(long)
     */
    public CdtContextInvertableBuilder<T> onListValue(Map<?,?> value) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE, Value.get(value)));
    }

    /**
     * Restrict to list elements from {@code index} through the end of the list.
     *
     * @param index starting index
     * @return builder for list CDT operations on that index range
     */
    public CdtActionInvertableBuilder<T> onListIndexRange(int index) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index));
    }

    /**
     * Restrict to {@code count} list elements starting at {@code index}.
     *
     * @param index starting index
     * @param count number of elements
     * @return builder for list CDT operations on that index range
     */
    public CdtActionInvertableBuilder<T> onListIndexRange(int index, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_INDEX_RANGE, index, count));
    }

    /**
     * Restrict to list elements from {@code rank} through the end of the list.
     *
     * @param rank starting rank
     * @return builder for list CDT operations on that rank range
     */
    public CdtActionInvertableBuilder<T> onListRankRange(int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank));
    }

    /**
     * Restrict to {@code count} list elements starting at {@code rank}.
     *
     * @param rank  starting rank
     * @param count number of elements
     * @return builder for list CDT operations on that rank range
     */
    public CdtActionInvertableBuilder<T> onListRankRange(int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_RANK_RANGE, rank, count));
    }

    /**
     * Restrict to list values in {@code [startIncl, endExcl)} (ordered comparison).
     *
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     */
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, long endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, String endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, byte[] endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, double endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(long startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(String startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(byte[] startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(double startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, boolean endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(boolean startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, List<?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(List<?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(SpecialValue startIncl, Map<?,?> endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, startIncl.toAerospikeValue(), Value.get(endExcl)));
    }

    /**
     * @param startIncl inclusive start
     * @param endExcl   exclusive end
     * @return builder for list CDT operations on that value range
     * @see #onListValueRange(long, long)
     */
    public CdtActionInvertableBuilder<T> onListValueRange(Map<?,?> startIncl, SpecialValue endExcl) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_RANGE, Value.get(startIncl), endExcl.toAerospikeValue()));
    }

    /**
     * Select list elements matching any of the given values.
     *
     * @param values candidate values
     * @return builder for nested list CDT operations on matching elements
     */
    public CdtContextInvertableBuilder<T> onListValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        CdtOperationParams params = new CdtOperationParams(CdtOperation.LIST_BY_VALUE_LIST, valueList);
        return new CdtGetOrRemoveBuilder<>(this.binName, this.opBuilder, params);
    }

    /**
     * Navigate by value-relative rank (see server CDT list by value relative rank range).
     *
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank));
    }

    /**
     * Like {@link #onListValueRelativeRankRange(long, int)} with a maximum number of elements.
     *
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(long value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(String value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(byte[] value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(double value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(SpecialValue value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, value.toAerospikeValue(), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(boolean value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(List<?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * @param value reference value
     * @param rank  relative rank offset
     * @param count max elements
     * @return builder for continued chaining (invertable for range operations)
     * @see #onListValueRelativeRankRange(long, int, int)
     */
    public CdtActionInvertableBuilder<T> onListValueRelativeRankRange(Map<?,?> value, int rank, int count) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, new CdtOperationParams(CdtOperation.LIST_BY_VALUE_REL_RANK_RANGE, Value.get(value), rank, count));
    }

    /**
     * Navigate to map items by a list of keys.
     *
     * @param keys the list of keys to match
     * @return builder for continued chaining
     */
    public CdtContextInvertableBuilder<T> onMapKeyList(List<?> keys) {
        List<Value> valueList = new ArrayList<>();
        for (Object key : keys) {
            valueList.add(Value.get(key));
        }
        CdtOperationParams params = new CdtOperationParams(CdtOperation.MAP_BY_KEY_LIST, valueList);
        return new CdtGetOrRemoveBuilder<>(this.binName, this.opBuilder, params);
    }

    /**
     * Navigate to map items by a list of values.
     *
     * @param values the list of values to match
     * @return builder for continued chaining
     */
    public CdtContextInvertableBuilder<T> onMapValueList(List<?> values) {
        List<Value> valueList = new ArrayList<>();
        for (Object value : values) {
            valueList.add(Value.get(value));
        }
        CdtOperationParams params = new CdtOperationParams(CdtOperation.MAP_BY_VALUE_LIST, valueList);
        return new CdtGetOrRemoveBuilder<>(this.binName, this.opBuilder, params);
    }

}