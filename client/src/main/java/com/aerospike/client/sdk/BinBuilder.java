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
import com.aerospike.client.sdk.Value.HLLValue;
import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.cdt.path.CdtPathExpressionAel;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.ExpReadFlags;
import com.aerospike.client.sdk.exp.ExpWriteFlags;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.operation.BitOperation;
import com.aerospike.client.sdk.operation.BitOverflowAction;
import com.aerospike.client.sdk.operation.BitPolicy;
import com.aerospike.client.sdk.operation.BitResizeFlags;
import com.aerospike.client.sdk.operation.HLLOperation;
import com.aerospike.client.sdk.operation.HLLWriteFlags;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.operation.StringRegexFlags;
import com.aerospike.client.sdk.operation.StringWriteFlags;
import com.aerospike.client.sdk.query.PreparedAel;

/**
 * Operations for one bin: scalar writes ({@link #setTo}), reads ({@link #get}),
 * server string read/modify ops (server 8.1.3+; fluent methods such as {@link #strlen},
 * {@link #substr}, {@link #find} delegate to {@link com.aerospike.client.sdk.operation.StringOperation};
 * see also {@code docs/string-operations.md} in the SDK repo for AEL cross-reference), numeric {@link #add},
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
     * Queues a write that sets this bin from a runtime {@link Object}.
     *
     * <p>Use this when the value type is not known statically (for example copying bins between
     * records). The object is converted with {@link Value#get(Object)}, which accepts the same
     * types as the typed {@code setTo} overloads plus boxed numbers, {@link Value}, enums, and
     * {@link java.util.UUID}. Prefer a typed {@code setTo} overload when the compile-time type is
     * known.</p>
     *
     * <p>{@code null} stores a null bin (same as {@link #remove()}), not an empty string.
     * Unsupported types throw {@link AerospikeException}.</p>
     *
     * <pre>{@code
     * Object value = rec.getValue("name");
     * session.upsert(destKey).bin("name").setTo(value).execute();
     * }</pre>
     *
     * @param value value to store, or {@code null} to write a null bin
     * @return the parent operation builder for chaining
     * @throws AerospikeException if {@code value}'s type cannot be stored in a bin
     */
    public T setTo(Object value) {
        return opBuilder.setTo(new Bin(binName, Value.get(value)));
    }

    /**
     * Queues a write that sets this bin to an already-constructed {@link Value}.
     *
     * <p>Use this when you already have a {@link Value} (for example from {@link Value#get(Object)}
     * or a GeoJSON/HLL constructor). {@code null} is treated as {@link Value#getAsNull()}.</p>
     *
     * <pre>{@code
     * session.upsert(key).bin("loc").setTo(Value.getAsGeoJSON(geoJson)).execute();
     * }</pre>
     *
     * @param value bin value to store, or {@code null} for a null bin
     * @return the parent operation builder for chaining
     */
    public T setTo(Value value) {
        return opBuilder.setTo(new Bin(binName, value != null ? value : Value.getAsNull()));
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
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, ExpReadFlags.DEFAULT, opBuilder.getSession().getCluster()));
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
        return selectFrom(ael, opts);
    }

    /**
     * Read a computed value with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #selectFrom(String, Consumer)}. Use it when
     * read options have already been built (for example shared across many bins) rather than
     * configured via a lambda.</p>
     *
     * @param ael     the AEL expression string
     * @param options the read options to apply (e.g. configured via
     *                {@link ExpressionReadOptions#ignoreEvalFailure()})
     * @return the parent operation builder for chaining
     */
    public T selectFrom(String ael, ExpressionReadOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return selectFrom(ael, opts);
    }

    /**
     * Read a computed value using a {@link BooleanExpression} with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #selectFrom(BooleanExpression, Consumer)}.</p>
     *
     * @param ael     the boolean expression to evaluate
     * @param options the read options to apply
     * @return the parent operation builder for chaining
     */
    public T selectFrom(BooleanExpression ael, ExpressionReadOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, options.getFlags()));
    }

    /**
     * Read a computed value using a PreparedAel with bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, params, ExpReadFlags.DEFAULT, opBuilder.getSession().getCluster()));
    }

    /**
     * Read a computed value using a PreparedAel with options and bound parameters.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.a / ?0");
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
        return selectFrom(ael, opts, params);
    }

    /**
     * Read a computed value using a {@link PreparedAel} with pre-built options and bound parameters.
     *
     * <p>This is the direct-options overload of {@link #selectFrom(PreparedAel, Consumer, Object...)}.</p>
     *
     * @param ael     the prepared AEL statement
     * @param options the read options to apply
     * @param params  parameter values to bind to the prepared statement
     * @return the parent operation builder for chaining
     */
    public T selectFrom(PreparedAel ael, ExpressionReadOptions options, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, ael, params, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return selectFrom(exp, opts);
    }

    /**
     * Read a computed value using a low-level {@link Exp} expression with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #selectFrom(Exp, Consumer)}.</p>
     *
     * @param exp     the Exp expression to evaluate
     * @param options the read options to apply
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Exp exp, ExpressionReadOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, options.getFlags()));
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
        return selectFrom(exp, opts);
    }

    /**
     * Read a computed value using a pre-compiled {@link Expression} with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #selectFrom(Expression, Consumer)}.</p>
     *
     * @param exp     the compiled expression to evaluate
     * @param options the read options to apply
     * @return the parent operation builder for chaining
     */
    public T selectFrom(Expression exp, ExpressionReadOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createReadOp(binName, exp, options.getFlags()));
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.CREATE_ONLY, opBuilder.getSession().getCluster()));
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
        return insertFrom(ael, opts);
    }

    /**
     * Write expression result only if the bin does not exist, with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #insertFrom(String, Consumer)}. The caller
     * is responsible for constructing the options &mdash; typically with
     * {@code new ExpressionWriteOptions(ExpWriteFlags.CREATE_ONLY)} &mdash; so the
     * {@code CREATE_ONLY} semantics implied by {@code insertFrom} are preserved.</p>
     *
     * @param ael     the AEL expression string
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T insertFrom(String ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return insertFrom(ael, opts);
    }

    /**
     * Write expression result only if the bin does not exist, using a {@link BooleanExpression}
     * with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #insertFrom(BooleanExpression, Consumer)}.
     * The caller is responsible for ensuring the options carry the desired write flags (the
     * Consumer overload pre-seeds {@code CREATE_ONLY}).</p>
     *
     * @param ael     the boolean expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T insertFrom(BooleanExpression ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a PreparedAel.
     * Fails with BIN_EXISTS_ERROR if the bin already exists.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.CREATE_ONLY, opBuilder.getSession().getCluster()));
    }

    /**
     * Write expression result only if bin doesn't exist, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return insertFrom(ael, opts, params);
    }

    /**
     * Write expression result only if the bin does not exist, using a {@link PreparedAel} with
     * pre-built options and bound parameters.
     *
     * <p>This is the direct-options overload of {@link #insertFrom(PreparedAel, Consumer, Object...)}.
     * The caller is responsible for ensuring the options carry the desired write flags (the
     * Consumer overload pre-seeds {@code CREATE_ONLY}).</p>
     *
     * @param ael     the prepared AEL statement
     * @param options the write options to apply
     * @param params  parameter values to bind to the prepared statement
     * @return the parent operation builder for chaining
     */
    public T insertFrom(PreparedAel ael, ExpressionWriteOptions options, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return insertFrom(exp, opts);
    }

    /**
     * Write expression result only if the bin does not exist, using a low-level {@link Exp} with
     * pre-built options.
     *
     * <p>This is the direct-options overload of {@link #insertFrom(Exp, Consumer)}. The caller is
     * responsible for ensuring the options carry the desired write flags (the Consumer overload
     * pre-seeds {@code CREATE_ONLY}).</p>
     *
     * @param exp     the Exp expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Exp exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
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
        return insertFrom(exp, opts);
    }

    /**
     * Write expression result only if the bin does not exist, using a pre-compiled
     * {@link Expression} with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #insertFrom(Expression, Consumer)}. The
     * caller is responsible for ensuring the options carry the desired write flags (the Consumer
     * overload pre-seeds {@code CREATE_ONLY}).</p>
     *
     * @param exp     the compiled expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T insertFrom(Expression exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.UPDATE_ONLY, opBuilder.getSession().getCluster()));
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
        return updateFrom(ael, opts);
    }

    /**
     * Write expression result only if the bin already exists, with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #updateFrom(String, Consumer)}. The caller
     * is responsible for constructing the options &mdash; typically with
     * {@code new ExpressionWriteOptions(ExpWriteFlags.UPDATE_ONLY)} &mdash; so the
     * {@code UPDATE_ONLY} semantics implied by {@code updateFrom} are preserved.</p>
     *
     * @param ael     the AEL expression string
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T updateFrom(String ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return updateFrom(ael, opts);
    }

    /**
     * Write expression result only if the bin already exists, using a {@link BooleanExpression}
     * with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #updateFrom(BooleanExpression, Consumer)}.
     * The caller is responsible for ensuring the options carry the desired write flags (the
     * Consumer overload pre-seeds {@code UPDATE_ONLY}).</p>
     *
     * @param ael     the boolean expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T updateFrom(BooleanExpression ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags()));
    }

    /**
     * Write expression result only if bin already exists, using a PreparedAel.
     * Fails with BIN_NOT_FOUND if the bin doesn't exist.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.UPDATE_ONLY, opBuilder.getSession().getCluster()));
    }

    /**
     * Write expression result only if bin already exists, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return updateFrom(ael, opts, params);
    }

    /**
     * Write expression result only if the bin already exists, using a {@link PreparedAel} with
     * pre-built options and bound parameters.
     *
     * <p>This is the direct-options overload of {@link #updateFrom(PreparedAel, Consumer, Object...)}.
     * The caller is responsible for ensuring the options carry the desired write flags (the
     * Consumer overload pre-seeds {@code UPDATE_ONLY}).</p>
     *
     * @param ael     the prepared AEL statement
     * @param options the write options to apply
     * @param params  parameter values to bind to the prepared statement
     * @return the parent operation builder for chaining
     */
    public T updateFrom(PreparedAel ael, ExpressionWriteOptions options, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return updateFrom(exp, opts);
    }

    /**
     * Write expression result only if the bin already exists, using a low-level {@link Exp} with
     * pre-built options.
     *
     * <p>This is the direct-options overload of {@link #updateFrom(Exp, Consumer)}. The caller is
     * responsible for ensuring the options carry the desired write flags (the Consumer overload
     * pre-seeds {@code UPDATE_ONLY}).</p>
     *
     * @param exp     the Exp expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Exp exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
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
        return updateFrom(exp, opts);
    }

    /**
     * Write expression result only if the bin already exists, using a pre-compiled
     * {@link Expression} with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #updateFrom(Expression, Consumer)}. The
     * caller is responsible for ensuring the options carry the desired write flags (the Consumer
     * overload pre-seeds {@code UPDATE_ONLY}).</p>
     *
     * @param exp     the compiled expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T updateFrom(Expression exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, ExpWriteFlags.DEFAULT, opBuilder.getSession().getCluster()));
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
        return upsertFrom(ael, opts);
    }

    /**
     * Write expression result, creating or overwriting the bin as needed, with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #upsertFrom(String, Consumer)}. Caller-built
     * options replace the lambda-based form. The Consumer overload initializes with
     * {@code ExpWriteFlags.DEFAULT}; the direct overload uses the supplied flags as-is.</p>
     *
     * @param ael     the AEL expression string
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(String ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return upsertFrom(ael, opts);
    }

    /**
     * Write expression result, creating or updating the bin, using a {@link BooleanExpression}
     * with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #upsertFrom(BooleanExpression, Consumer)}.</p>
     *
     * @param ael     the boolean expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(BooleanExpression ael, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, options.getFlags()));
    }

    /**
     * Write expression result, creating or updating the bin, using a PreparedAel.
     * Never fails due to bin existence.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, ExpWriteFlags.DEFAULT, opBuilder.getSession().getCluster()));
    }

    /**
     * Write expression result, creating or updating the bin, using a PreparedAel with options.
     *
     * <pre>{@code
     * PreparedAel calc = PreparedAel.prepare("$.price * ?0");
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
        return upsertFrom(ael, opts, params);
    }

    /**
     * Write expression result, creating or updating the bin, using a {@link PreparedAel} with
     * pre-built options and bound parameters.
     *
     * <p>This is the direct-options overload of {@link #upsertFrom(PreparedAel, Consumer, Object...)}.</p>
     *
     * @param ael     the prepared AEL statement
     * @param options the write options to apply
     * @param params  parameter values to bind to the prepared statement
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(PreparedAel ael, ExpressionWriteOptions options, Object... params) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, ael, params, options.getFlags(), opBuilder.getSession().getCluster()));
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
        return upsertFrom(exp, opts);
    }

    /**
     * Write expression result, creating or updating the bin, using a low-level {@link Exp} with
     * pre-built options.
     *
     * <p>This is the direct-options overload of {@link #upsertFrom(Exp, Consumer)}.</p>
     *
     * @param exp     the Exp expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Exp exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
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
        return upsertFrom(exp, opts);
    }

    /**
     * Write expression result, creating or updating the bin, using a pre-compiled
     * {@link Expression} with pre-built options.
     *
     * <p>This is the direct-options overload of {@link #upsertFrom(Expression, Consumer)}.</p>
     *
     * @param exp     the compiled expression to evaluate
     * @param options the write options to apply
     * @return the parent operation builder for chaining
     */
    public T upsertFrom(Expression exp, ExpressionWriteOptions options) {
        return opBuilder.addOp(ExpressionOpHelper.createWriteOp(binName, exp, options.getFlags()));
    }

    // ==================================================================
    // CDT Operations. Note: make sure to mirror these operations to
    // CdtContextNonInvertableBuilder and CdtContextInvertableBuilder
    // ==================================================================

    /**
     * Begin path iteration at the <strong>bin root</strong>: every direct child of this bin is visited
     * ({@link com.aerospike.client.sdk.cdt.CTX#allChildren()}), then continue with {@code onMapKey},
     * {@code onEachChild}, or a terminal such as {@link CdtContextNonInvertableBuilder#collectValues()}.
     *
     * <p>Requires server <strong>8.1.1+</strong> for path expressions. Equivalent to starting a
     * {@link CdtGetOrRemoveBuilder} with {@link CdtOperationParams#forEachChildAtBinRoot()}.</p>
     *
     * <p><b>Example</b>:</p>
     * <pre>{@code
     * session.upsert(key).bin("nums").onEachChild().modifyBy(Exp.add(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(1)))
     *     .execute();
     * }</pre>
     *
     * @return nested CDT path builder rooted at this bin name
     * @see CdtContextNonInvertableBuilder#onEachChild()
     */
    public CdtContextNonInvertableBuilder<T> onEachChild() {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, CdtOperationParams.forEachChildAtBinRoot());
    }

    /**
     * Same as {@link #onEachChild()} but only children matching {@code filter} are visited
     * ({@link com.aerospike.client.sdk.cdt.CTX#allChildrenWithFilter(com.aerospike.client.sdk.exp.Exp)}).
     *
     * @param filter predicate evaluated for each child at the bin root
     * @return nested CDT path builder for this bin
     * @see CdtContextNonInvertableBuilder#onEachChild(Exp)
     */
    public CdtContextNonInvertableBuilder<T> onEachChild(Exp filter) {
        return new CdtGetOrRemoveBuilder<>(binName, opBuilder, CdtOperationParams.forEachChildAtBinRootWithFilter(filter));
    }

    /**
     * Same as {@link #onEachChild(Exp)} with the filter expressed as AEL.
     *
     * @param ael AEL predicate for filtered iteration
     * @return nested CDT path builder (unreachable until supported)
     * @throws UnsupportedOperationException until path-scoped AEL is implemented
     * @see CdtContextNonInvertableBuilder#onEachChild(String)
     */
    public CdtContextNonInvertableBuilder<T> onEachChild(String ael) {
        CdtPathExpressionAel.throwAelNotSupported();
        throw new AssertionError("unreachable");
    }

    /**
     * Same as {@link #onEachChild(String)} using {@link PreparedAel} and bind parameters.
     *
     * @param ael prepared AEL template
     * @param bindParams bind values for {@code ael}
     * @return nested CDT path builder (unreachable until supported)
     * @throws UnsupportedOperationException until path-scoped AEL is implemented
     * @see CdtContextNonInvertableBuilder#onEachChild(PreparedAel, Object...)
     */
    public CdtContextNonInvertableBuilder<T> onEachChild(PreparedAel ael, Object... bindParams) {
        CdtPathExpressionAel.throwPreparedAelNotSupported(ael, bindParams);
        throw new AssertionError("unreachable");
    }

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

    // ----------------------------------------
    // String server operations (server 8.1.3+)
    // ----------------------------------------

    /**
     * Queues a string {@code strlen} read: Unicode codepoint count of this bin.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#strlen(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T strlen() {
        return opBuilder.addOp(StringOperation.strlen(binName));
    }

    /**
     * Queues a string {@code substr} read from {@code start} through the end of the string.
     * Negative {@code start} counts from the end.
     *
     * @param start starting codepoint index (inclusive; negative counts from end)
     * @return the parent operation builder for chaining
     * @see StringOperation#substr(String, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T substr(int start) {
        return opBuilder.addOp(StringOperation.substr(binName, start));
    }

    /**
     * Queues a string {@code substr} read for the half-open codepoint range {@code [start, end)}.
     *
     * @param start first codepoint index (inclusive; negative counts from end)
     * @param end   one past the last codepoint (exclusive)
     * @return the parent operation builder for chaining
     * @see StringOperation#substr(String, int, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T substr(int start, int end) {
        return opBuilder.addOp(StringOperation.substr(binName, start, end));
    }

    /**
     * Queues a string {@code charAt} read: single codepoint at {@code index}.
     *
     * @param index codepoint index (negative counts from end)
     * @return the parent operation builder for chaining
     * @see StringOperation#charAt(String, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T charAt(int index) {
        return opBuilder.addOp(StringOperation.charAt(binName, index));
    }

    /**
     * Queues a string {@code find} read: index of the first occurrence of {@code needle}, or -1.
     *
     * @param needle substring to search for
     * @return the parent operation builder for chaining
     * @see StringOperation#find(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T find(String needle) {
        return opBuilder.addOp(StringOperation.find(binName, needle));
    }

    /**
     * Queues a string {@code find} read for the {@code occurrence}-th match of {@code needle}
     * ({@code 1} = first, {@code -1} = last), or -1 if not found.
     *
     * @param needle       substring to search for
     * @param occurrence   1-based occurrence index (negative counts from the last match)
     * @return the parent operation builder for chaining
     * @see StringOperation#find(String, String, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T find(String needle, int occurrence) {
        return opBuilder.addOp(StringOperation.find(binName, needle, occurrence));
    }

    /**
     * Queues a string {@code contains} read.
     *
     * @param needle substring to test for
     * @return the parent operation builder for chaining
     * @see StringOperation#contains(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T contains(String needle) {
        return opBuilder.addOp(StringOperation.contains(binName, needle));
    }

    /**
     * Queues a string {@code startsWith} read.
     *
     * @param prefix prefix to test
     * @return the parent operation builder for chaining
     * @see StringOperation#startsWith(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T startsWith(String prefix) {
        return opBuilder.addOp(StringOperation.startsWith(binName, prefix));
    }

    /**
     * Queues a string {@code endsWith} read.
     *
     * @param suffix suffix to test
     * @return the parent operation builder for chaining
     * @see StringOperation#endsWith(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T endsWith(String suffix) {
        return opBuilder.addOp(StringOperation.endsWith(binName, suffix));
    }

    /**
     * Queues a string {@code toInteger} read: parse bin as int64.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#toInteger(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T stringToInteger() {
        return opBuilder.addOp(StringOperation.toInteger(binName));
    }

    /**
     * Queues a string {@code toDouble} read: parse bin as float64.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#toDouble(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T stringToDouble() {
        return opBuilder.addOp(StringOperation.toDouble(binName));
    }

    /**
     * Queues a string {@code byteLength} read: UTF-8 byte length.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#byteLength(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T byteLength() {
        return opBuilder.addOp(StringOperation.byteLength(binName));
    }

    /**
     * Queues a string {@code isNumeric} read.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#isNumeric(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T isNumeric() {
        return opBuilder.addOp(StringOperation.isNumeric(binName));
    }

    /**
     * Queues a string {@code isNumeric} read filtered by {@code numericType}
     * ({@link com.aerospike.client.sdk.operation.StringNumericType} constants).
     *
     * @param numericType {@code ANY}, {@code INT}, or {@code FLOAT}
     * @return the parent operation builder for chaining
     * @see StringOperation#isNumeric(String, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T isNumeric(int numericType) {
        return opBuilder.addOp(StringOperation.isNumeric(binName, numericType));
    }

    /**
     * Queues a string {@code isUpper} read.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#isUpper(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T isUpper() {
        return opBuilder.addOp(StringOperation.isUpper(binName));
    }

    /**
     * Queues a string {@code isLower} read.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#isLower(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T isLower() {
        return opBuilder.addOp(StringOperation.isLower(binName));
    }

    /**
     * Queues a string {@code toBlob} read: UTF-8 bytes as blob.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#toBlob(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T stringToBlob() {
        return opBuilder.addOp(StringOperation.toBlob(binName));
    }

    /**
     * Queues a string {@code split} read with default separator (server-defined).
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#split(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T split() {
        return opBuilder.addOp(StringOperation.split(binName));
    }

    /**
     * Queues a string {@code split} read using {@code separator}.
     *
     * @param separator delimiter (empty splits per codepoint, per server rules)
     * @return the parent operation builder for chaining
     * @see StringOperation#split(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T split(String separator) {
        return opBuilder.addOp(StringOperation.split(binName, separator));
    }

    /**
     * Queues a string {@code b64Decode} read: base64 string to blob.
     *
     * @return the parent operation builder for chaining
     * @see StringOperation#b64Decode(String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T b64Decode() {
        return opBuilder.addOp(StringOperation.b64Decode(binName));
    }

    /**
     * Queues a string {@code regexCompare} read with default regex flags.
     *
     * @param pattern ICU-syntax regex pattern
     * @return the parent operation builder for chaining
     * @see StringOperation#regexCompare(String, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T regexCompare(String pattern) {
        return opBuilder.addOp(StringOperation.regexCompare(binName, pattern));
    }

    /**
     * Queues a string {@code regexCompare} read with {@link StringRegexFlags}.
     *
     * @param pattern    ICU-syntax regex pattern
     * @param regexFlags bitwise-OR of {@link StringRegexFlags}
     * @return the parent operation builder for chaining
     * @see StringOperation#regexCompare(String, String, int, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T regexCompare(String pattern, int regexFlags) {
        return opBuilder.addOp(StringOperation.regexCompare(binName, pattern, regexFlags));
    }

    /**
     * Queues {@link StringOperation#toString(String)}: string representation of this bin's
     * value (int, float, string, bool, or valid UTF-8 blob). Not a string sub-op; no nested CTX.
     *
     * @return the parent operation builder for chaining
     */
    public T readAsString() {
        return opBuilder.addOp(StringOperation.toString(binName));
    }

    private T addStringModifyOp(Operation op) {
        return opBuilder.addOp(op);
    }

    /**
     * Queues string {@code insert} modify at {@code index}.
     *
     * @param index codepoint index (negative counts from end)
     * @param value text to insert
     * @return the parent operation builder for chaining
     * @see StringOperation#insert(int, String, int, String, com.aerospike.client.sdk.cdt.CTX...)
     */
    public T insert(int index, String value) {
        return addStringModifyOp(StringOperation.insert(StringWriteFlags.DEFAULT, binName, index, value));
    }

    /**
     * Queues string {@code insert} with {@link StringWriteOptions}.
     */
    public T insert(int index, String value, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return insert(index, value, o);
    }

    /**
     * Queues string {@code insert} with pre-built {@link StringWriteOptions}.
     */
    public T insert(int index, String value, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.insert(options.toFlags(), binName, index, value));
    }

    /**
     * Queues string {@code overwrite} modify at {@code index}.
     */
    public T overwrite(int index, String value) {
        return addStringModifyOp(StringOperation.overwrite(StringWriteFlags.DEFAULT, binName, index, value));
    }

    public T overwrite(int index, String value, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return overwrite(index, value, o);
    }

    public T overwrite(int index, String value, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.overwrite(options.toFlags(), binName, index, value));
    }

    /**
     * Queues string {@code concat} modify: append {@code fragment}.
     */
    public T stringConcat(String fragment) {
        return addStringModifyOp(StringOperation.concat(StringWriteFlags.DEFAULT, binName, fragment));
    }

    public T stringConcat(String fragment, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return stringConcat(fragment, o);
    }

    public T stringConcat(String fragment, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.concat(options.toFlags(), binName, fragment));
    }

    /**
     * Queues string {@code concat} modify: append all strings in order.
     */
    public T stringConcat(List<String> fragments) {
        return addStringModifyOp(StringOperation.concat(StringWriteFlags.DEFAULT, binName, fragments));
    }

    public T stringConcat(List<String> fragments, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return stringConcat(fragments, o);
    }

    public T stringConcat(List<String> fragments, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.concat(options.toFlags(), binName, fragments));
    }

    /**
     * Queues a string append on this bin (bin must hold a string).
     *
     * @param fragment text to append
     * @return the parent operation builder for chaining
     */
    public T append(String fragment) {
        if (opBuilder.session.getCluster().supportsStringOperations()) {
            return addStringModifyOp(StringOperation.append(StringWriteFlags.DEFAULT, binName, fragment));
        }
        else {
            return opBuilder.append(new Bin(binName, fragment));
        }
    }

    public T append(String fragment, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return append(fragment, o);
    }

    public T append(String fragment, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.append(options.toFlags(), binName, fragment));
    }

    /**
     * Queues a string prepend on this bin (bin must hold a string).
     *
     * @param fragment text to prepend
     * @return the parent operation builder for chaining
     */
    public T prepend(String fragment) {
        if (opBuilder.session.getCluster().supportsStringOperations()) {
            return addStringModifyOp(StringOperation.prepend(StringWriteFlags.DEFAULT, binName, fragment));
        }
        else {
            return opBuilder.prepend(new Bin(binName, fragment));
        }
    }

    public T prepend(String fragment, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return prepend(fragment, o);
    }

    public T prepend(String fragment, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.prepend(options.toFlags(), binName, fragment));
    }

    /**
     * Queues string {@code snip} modify: remove half-open range {@code [start, end)}.
     */
    public T snip(int start, int end) {
        return addStringModifyOp(StringOperation.snip(StringWriteFlags.DEFAULT, binName, start, end));
    }

    public T snip(int start, int end, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return snip(start, end, o);
    }

    public T snip(int start, int end, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.snip(options.toFlags(), binName, start, end));
    }

    /**
     * Queues string {@code replace} modify (first occurrence only).
     */
    public T replace(String needle, String replacement) {
        return addStringModifyOp(StringOperation.replace(StringWriteFlags.DEFAULT, binName, needle, replacement));
    }

    public T replace(String needle, String replacement, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return replace(needle, replacement, o);
    }

    public T replace(String needle, String replacement, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.replace(options.toFlags(), binName, needle, replacement));
    }

    /**
     * Queues string {@code replaceAll} modify.
     */
    public T replaceAll(String needle, String replacement) {
        return addStringModifyOp(StringOperation.replaceAll(StringWriteFlags.DEFAULT, binName, needle, replacement));
    }

    public T replaceAll(String needle, String replacement, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return replaceAll(needle, replacement, o);
    }

    public T replaceAll(String needle, String replacement, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.replaceAll(options.toFlags(), binName, needle, replacement));
    }

    /** Queues string {@code upper} modify. */
    public T upper() {
        return addStringModifyOp(StringOperation.upper(StringWriteFlags.DEFAULT, binName));
    }

    public T upper(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return upper(o);
    }

    public T upper(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.upper(options.toFlags(), binName));
    }

    /** Queues string {@code lower} modify. */
    public T lower() {
        return addStringModifyOp(StringOperation.lower(StringWriteFlags.DEFAULT, binName));
    }

    public T lower(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return lower(o);
    }

    public T lower(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.lower(options.toFlags(), binName));
    }

    /** Queues string {@code caseFold} modify. */
    public T caseFold() {
        return addStringModifyOp(StringOperation.caseFold(StringWriteFlags.DEFAULT, binName));
    }

    public T caseFold(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return caseFold(o);
    }

    public T caseFold(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.caseFold(options.toFlags(), binName));
    }

    /** Queues string {@code normalizeNFC} modify. */
    public T normalizeNfc() {
        return addStringModifyOp(StringOperation.normalizeNFC(StringWriteFlags.DEFAULT, binName));
    }

    public T normalizeNfc(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return normalizeNfc(o);
    }

    public T normalizeNfc(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.normalizeNFC(options.toFlags(), binName));
    }

    /** Queues string {@code trimStart} modify. */
    public T trimStart() {
        return addStringModifyOp(StringOperation.trimStart(StringWriteFlags.DEFAULT, binName));
    }

    public T trimStart(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return trimStart(o);
    }

    public T trimStart(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.trimStart(options.toFlags(), binName));
    }

    /** Queues string {@code trimEnd} modify. */
    public T trimEnd() {
        return addStringModifyOp(StringOperation.trimEnd(StringWriteFlags.DEFAULT, binName));
    }

    public T trimEnd(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return trimEnd(o);
    }

    public T trimEnd(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.trimEnd(options.toFlags(), binName));
    }

    /** Queues string {@code trim} modify (both ends). */
    public T trim() {
        return addStringModifyOp(StringOperation.trim(StringWriteFlags.DEFAULT, binName));
    }

    public T trim(Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return trim(o);
    }

    public T trim(StringWriteOptions options) {
        return addStringModifyOp(StringOperation.trim(options.toFlags(), binName));
    }

    /**
     * Queues string {@code padStart} modify: pad to at least {@code targetLength} codepoints.
     */
    public T padStart(int targetLength, String padString) {
        return addStringModifyOp(StringOperation.padStart(StringWriteFlags.DEFAULT, binName, targetLength, padString));
    }

    public T padStart(int targetLength, String padString, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return padStart(targetLength, padString, o);
    }

    public T padStart(int targetLength, String padString, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.padStart(options.toFlags(), binName, targetLength, padString));
    }

    /**
     * Queues string {@code padEnd} modify.
     */
    public T padEnd(int targetLength, String padString) {
        return addStringModifyOp(StringOperation.padEnd(StringWriteFlags.DEFAULT, binName, targetLength, padString));
    }

    public T padEnd(int targetLength, String padString, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return padEnd(targetLength, padString, o);
    }

    public T padEnd(int targetLength, String padString, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.padEnd(options.toFlags(), binName, targetLength, padString));
    }

    /** Queues string {@code repeat} modify. */
    public T repeat(int count) {
        return addStringModifyOp(StringOperation.repeat(StringWriteFlags.DEFAULT, binName, count));
    }

    public T repeat(int count, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return repeat(count, o);
    }

    public T repeat(int count, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.repeat(options.toFlags(), binName, count));
    }

    /**
     * Queues string {@code regexReplace} modify with default regex flags.
     */
    public T regexReplace(String pattern, String replacement) {
        return addStringModifyOp(StringOperation.regexReplace(StringWriteFlags.DEFAULT, binName, pattern,
            replacement, StringRegexFlags.DEFAULT));
    }

    public T regexReplace(String pattern, String replacement, Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return regexReplace(pattern, replacement, StringRegexFlags.DEFAULT, o);
    }

    /**
     * Queues string {@code regexReplace} with default regex flags and {@link StringWriteOptions}.
     */
    public T regexReplace(String pattern, String replacement, StringWriteOptions options) {
        return regexReplace(pattern, replacement, StringRegexFlags.DEFAULT, options);
    }

    /**
     * Queues string {@code regexReplace} with {@link StringRegexFlags}.
     */
    public T regexReplace(String pattern, String replacement, int regexFlags) {
        return addStringModifyOp(StringOperation.regexReplace(StringWriteFlags.DEFAULT, binName, pattern,
            replacement, regexFlags));
    }

    public T regexReplace(String pattern, String replacement, int regexFlags,
        Consumer<StringWriteOptions> options) {
        StringWriteOptions o = new StringWriteOptions();
        options.accept(o);
        return regexReplace(pattern, replacement, regexFlags, o);
    }

    public T regexReplace(String pattern, String replacement, int regexFlags, StringWriteOptions options) {
        return addStringModifyOp(StringOperation.regexReplace(options.toFlags(), binName, pattern,
            replacement, regexFlags));
    }

    // ----------------------------------------
    // HyperLogLog (HLL)
    // ----------------------------------------

    /**
     * Queues a write that sets this bin to a HLLValue.
     *
     * @param value HLL value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(Value.HLLValue value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Initialize the HLL bin (write).
     *
     * <p>Creates a new HyperLogLog bin or resets an existing one using the index
     * bit count (and optional minhash bit count) from {@code config}. Uses the
     * default write mode; the server returns no value.</p>
     *
     * @param config HLL bin configuration (index/minhash bit counts)
     * @return builder for continued chaining
     * @see HllConfig
     */
    public T hllInit(HllConfig config) {
        Operation op = HLLOperation.init(HLLWriteFlags.DEFAULT, binName, config.indexBitCount(),
            config.minHashBitCount());
        return opBuilder.addOp(op);
    }

    /**
     * Initialize the HLL bin with caller-supplied write options (write).
     *
     * <p>Creates a new HyperLogLog bin or resets an existing one using
     * {@code config}. The {@code options} lambda configures write semantics
     * such as {@code createOnly}, {@code updateOnly}, {@code noFail}, and
     * {@code allowFold}.</p>
     *
     * @param config  HLL bin configuration (index/minhash bit counts)
     * @param options consumer that configures the {@link HllWriteOptions}
     * @return builder for continued chaining
     * @see HllConfig
     * @see HllWriteOptions
     */
    public T hllInit(HllConfig config, Consumer<HllWriteOptions> options) {
        HllWriteOptions opts = new HllWriteOptions();
        options.accept(opts);
        return hllInit(config, opts);
    }

    /**
     * Initialize the HLL bin with pre-built write options (write).
     *
     * <p>This is the direct-options overload of {@link #hllInit(HllConfig, Consumer)}. Use it
     * when {@link HllWriteOptions} have already been built (for example shared across multiple
     * operations) rather than configured via a lambda.</p>
     *
     * @param config  HLL bin configuration (index/minhash bit counts)
     * @param options pre-built write options controlling semantics such as {@code createOnly},
     *                {@code updateOnly}, {@code noFail}, and {@code allowFold}
     * @return builder for continued chaining
     * @see HllConfig
     * @see HllWriteOptions
     */
    public T hllInit(HllConfig config, HllWriteOptions options) {
        Operation op = HLLOperation.init(options.toFlags(), binName, config.indexBitCount(),
            config.minHashBitCount());
        return opBuilder.addOp(op);
    }

    /**
     * Add values to an existing HLL bin (write).
     *
     * <p>Assumes the HLL bin already exists. The server returns the number of
     * entries that caused the HLL to update a register.</p>
     *
     * @param values values to add to the HLL set
     * @return builder for continued chaining
     */
    public T hllAdd(List<?> values) {
        Operation op = HLLOperation.add(HLLWriteFlags.DEFAULT, binName, values);
        return opBuilder.addOp(op);
    }

    /**
     * Add values to the HLL bin, creating it if it does not exist (write).
     *
     * <p>If the bin does not yet exist it is created using {@code config}.
     * The server returns the number of entries that caused the HLL to update
     * a register.</p>
     *
     * @param values values to add to the HLL set
     * @param config HLL bin configuration used to create the bin if missing
     * @return builder for continued chaining
     * @see HllConfig
     */
    public T hllAdd(List<?> values, HllConfig config) {
        Operation op = HLLOperation.add(HLLWriteFlags.DEFAULT, binName, values, config.indexBitCount(),
            config.minHashBitCount());
        return opBuilder.addOp(op);
    }

    /**
     * Add values to the HLL bin with caller-supplied write options, creating it
     * if it does not exist (write).
     *
     * <p>If the bin does not yet exist it is created using {@code config}. The
     * {@code options} lambda configures write semantics such as
     * {@code createOnly}, {@code updateOnly}, {@code noFail}, and
     * {@code allowFold}. The server returns the number of entries that caused
     * the HLL to update a register.</p>
     *
     * @param values  values to add to the HLL set
     * @param config  HLL bin configuration used to create the bin if missing
     * @param options consumer that configures the {@link HllWriteOptions}
     * @return builder for continued chaining
     * @see HllConfig
     * @see HllWriteOptions
     */
    public T hllAdd(List<?> values, HllConfig config, Consumer<HllWriteOptions> options) {
        HllWriteOptions opts = new HllWriteOptions();
        options.accept(opts);
        return hllAdd(values, config, opts);
    }

    /**
     * Add values to the HLL bin with pre-built write options, creating it if it does not exist
     * (write).
     *
     * <p>This is the direct-options overload of
     * {@link #hllAdd(List, HllConfig, Consumer)}. If the bin does not yet exist it is created
     * using {@code config}. The server returns the number of entries that caused the HLL to
     * update a register.</p>
     *
     * @param values  values to add to the HLL set
     * @param config  HLL bin configuration used to create the bin if missing
     * @param options pre-built write options controlling semantics such as {@code createOnly},
     *                {@code updateOnly}, {@code noFail}, and {@code allowFold}
     * @return builder for continued chaining
     * @see HllConfig
     * @see HllWriteOptions
     */
    public T hllAdd(List<?> values, HllConfig config, HllWriteOptions options) {
        Operation op = HLLOperation.add(options.toFlags(), binName, values, config.indexBitCount(),
            config.minHashBitCount());
        return opBuilder.addOp(op);
    }

    /**
     * Replace the HLL bin with the union of the supplied HLL values (write).
     *
     * <p>Server sets the bin to the union of {@code hlls} merged with the
     * existing bin contents. Uses the default write mode; the server returns
     * no value.</p>
     *
     * @param hlls HLL values to union into the bin
     * @return builder for continued chaining
     */
    public T hllSetUnion(List<HLLValue> hlls) {
        Operation op = HLLOperation.setUnion(HLLWriteFlags.DEFAULT, binName, hlls);
        return opBuilder.addOp(op);
    }

    /**
     * Replace the HLL bin with the union of the supplied HLL values, with
     * caller-supplied write options (write).
     *
     * <p>The {@code options} lambda configures write semantics such as
     * {@code createOnly}, {@code updateOnly}, {@code noFail}, and
     * {@code allowFold}.</p>
     *
     * @param hlls    HLL values to union into the bin
     * @param options consumer that configures the {@link HllWriteOptions}
     * @return builder for continued chaining
     * @see HllWriteOptions
     */
    public T hllSetUnion(List<HLLValue> hlls, Consumer<HllWriteOptions> options) {
        HllWriteOptions opts = new HllWriteOptions();
        options.accept(opts);
        return hllSetUnion(hlls, opts);
    }

    /**
     * Replace the HLL bin with the union of the supplied HLL values, with pre-built write options
     * (write).
     *
     * <p>This is the direct-options overload of {@link #hllSetUnion(List, Consumer)}. The
     * caller-built options replace the lambda-based form.</p>
     *
     * @param hlls    HLL values to union into the bin
     * @param options pre-built write options controlling semantics such as {@code createOnly},
     *                {@code updateOnly}, {@code noFail}, and {@code allowFold}
     * @return builder for continued chaining
     * @see HllWriteOptions
     */
    public T hllSetUnion(List<HLLValue> hlls, HllWriteOptions options) {
        Operation op = HLLOperation.setUnion(options.toFlags(), binName, hlls);
        return opBuilder.addOp(op);
    }

    /**
     * Fold the HLL bin to a smaller index bit count (write).
     *
     * <p>Reduces the precision of the HLL bin to {@code indexBitCount} index
     * bits. Can only be applied when the bin's minhash bit count is 0. The
     * server returns no value.</p>
     *
     * @param indexBitCount target number of index bits (4–16 inclusive); must
     *                      be less than or equal to the current index bit count
     * @return builder for continued chaining
     */
    public T hllFold(int indexBitCount) {
        Operation op = HLLOperation.fold(binName, indexBitCount);
        return opBuilder.addOp(op);
    }

    /**
     * Refresh the cached element count on the HLL bin (write).
     *
     * <p>Server updates the cached count if it is stale and returns the
     * refreshed count.</p>
     *
     * @return builder for continued chaining
     */
    public T hllRefreshCount() {
        Operation op = HLLOperation.refreshCount(binName);
        return opBuilder.addOp(op);
    }

    /**
     * Read the estimated cardinality of the HLL bin (read).
     *
     * <p>Server returns the estimated number of unique elements in the bin
     * as a long.</p>
     *
     * @return builder for continued chaining
     */
    public T hllGetCount() {
        Operation op = HLLOperation.getCount(binName);
        return opBuilder.addOp(op);
    }

    /**
     * Describe the HLL bin's configuration (read).
     *
     * <p>Server returns a list of two longs containing the {@code indexBitCount}
     * and {@code minHashBitCount} that were used to create the bin. See
     * {@link HllConfig} and {@code Record#getHllConfig(String)} for a typed view
     * of the result.</p>
     *
     * @return builder for continued chaining
     * @see HllConfig
     */
    public T hllDescribe() {
        Operation op = HLLOperation.describe(binName);
        return opBuilder.addOp(op);
    }

    /**
     * Read the union of the HLL bin with the supplied HLL values (read).
     *
     * <p>Server returns an HLL value that is the union of {@code hlls} together
     * with the bin's current contents. The bin itself is not modified.</p>
     *
     * @param hlls HLL values to union with the bin
     * @return builder for continued chaining
     */
    public T hllGetUnion(List<HLLValue> hlls) {
        Operation op = HLLOperation.getUnion(binName, hlls);
        return opBuilder.addOp(op);
    }

    /**
     * Read the estimated count of the union of the HLL bin with the supplied
     * HLL values (read).
     *
     * <p>Server returns the estimated number of unique elements in the union
     * of {@code hlls} with the bin's current contents. The bin itself is not
     * modified.</p>
     *
     * @param hlls HLL values to union with the bin
     * @return builder for continued chaining
     */
    public T hllGetUnionCount(List<HLLValue> hlls) {
        Operation op = HLLOperation.getUnionCount(binName, hlls);
        return opBuilder.addOp(op);
    }

    /**
     * Read the estimated count of the intersection of the HLL bin with the
     * supplied HLL values (read).
     *
     * <p>Server returns the estimated number of elements contained in the
     * intersection of {@code hlls} with the bin. The {@code hlls} list may
     * contain at most two values when minhash bits are 0; more are allowed
     * when minhash bits are nonzero.</p>
     *
     * @param hlls HLL values to intersect with the bin
     * @return builder for continued chaining
     */
    public T hllGetIntersectCount(List<HLLValue> hlls) {
        Operation op = HLLOperation.getIntersectCount(binName, hlls);
        return opBuilder.addOp(op);
    }

    /**
     * Read the estimated Jaccard similarity of the HLL bin with the supplied
     * HLL values (read).
     *
     * <p>Server returns a double in {@code [0.0, 1.0]} estimating the
     * similarity of the bin to {@code hlls}. The {@code hlls} list may
     * contain at most two values when minhash bits are 0; more are allowed
     * when minhash bits are nonzero.</p>
     *
     * @param hlls HLL values to compare against the bin
     * @return builder for continued chaining
     */
    public T hllGetSimilarity(List<HLLValue> hlls) {
        Operation op = HLLOperation.getSimilarity(binName, hlls);
        return opBuilder.addOp(op);
    }

    // ----------------------------------------
    // Bit (BLOB)
    // ----------------------------------------

    /**
     * Resize the BLOB bin to {@code byteSize} bytes (write).
     *
     * <p>Uses {@link BitResizeFlags#DEFAULT} resize semantics and default write policy.</p>
     *
     * @param byteSize target size of the blob in bytes
     * @return builder for continued chaining
     */
    public T bitResize(int byteSize) {
        return bitResize(byteSize, BitResizeFlags.DEFAULT);
    }

    /**
     * Resize the BLOB bin to {@code byteSize} bytes with caller-supplied resize flags (write).
     *
     * @param byteSize    target size of the blob in bytes
     * @param resizeFlags bitwise-OR of {@link BitResizeFlags}
     * @return builder for continued chaining
     */
    public T bitResize(int byteSize, int resizeFlags) {
        Operation op = BitOperation.resize(BitPolicy.Default, binName, byteSize, resizeFlags);
        return opBuilder.addOp(op);
    }

    /**
     * Resize the BLOB bin with caller-supplied resize flags and write options (write).
     *
     * @param byteSize    target size of the blob in bytes
     * @param resizeFlags bitwise-OR of {@link BitResizeFlags}
     * @param options     consumer that configures {@link BitWriteOptions}
     * @return builder for continued chaining
     */
    public T bitResize(int byteSize, int resizeFlags, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitResize(byteSize, resizeFlags, opts);
    }

    /**
     * Resize the BLOB bin with caller-supplied resize flags and pre-built write options (write).
     *
     * @param byteSize    target size of the blob in bytes
     * @param resizeFlags bitwise-OR of {@link BitResizeFlags}
     * @param options     pre-built write options
     * @return builder for continued chaining
     */
    public T bitResize(int byteSize, int resizeFlags, BitWriteOptions options) {
        Operation op = BitOperation.resize(bitPolicy(options), binName, byteSize, resizeFlags);
        return opBuilder.addOp(op);
    }

    /**
     * Insert {@code value} bytes at {@code byteOffset} in the BLOB bin (write).
     *
     * @param byteOffset byte position at which to insert
     * @param value      bytes to insert
     * @return builder for continued chaining
     */
    public T bitInsert(int byteOffset, byte[] value) {
        Operation op = BitOperation.insert(BitPolicy.Default, binName, byteOffset, value);
        return opBuilder.addOp(op);
    }

    /**
     * Insert bytes at {@code byteOffset} with caller-supplied write options (write).
     *
     * @param byteOffset byte position at which to insert
     * @param value      bytes to insert
     * @param options    consumer that configures {@link BitWriteOptions}
     * @return builder for continued chaining
     */
    public T bitInsert(int byteOffset, byte[] value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitInsert(byteOffset, value, opts);
    }

    /**
     * Insert bytes at {@code byteOffset} with pre-built write options (write).
     *
     * @param byteOffset byte position at which to insert
     * @param value      bytes to insert
     * @param options    pre-built write options
     * @return builder for continued chaining
     */
    public T bitInsert(int byteOffset, byte[] value, BitWriteOptions options) {
        Operation op = BitOperation.insert(bitPolicy(options), binName, byteOffset, value);
        return opBuilder.addOp(op);
    }

    /**
     * Remove {@code byteSize} bytes starting at {@code byteOffset} from the BLOB bin (write).
     *
     * @param byteOffset start of the range to remove
     * @param byteSize   number of bytes to remove
     * @return builder for continued chaining
     */
    public T bitRemove(int byteOffset, int byteSize) {
        Operation op = BitOperation.remove(BitPolicy.Default, binName, byteOffset, byteSize);
        return opBuilder.addOp(op);
    }

    /**
     * Remove bytes with caller-supplied write options (write).
     *
     * @param byteOffset start of the range to remove
     * @param byteSize   number of bytes to remove
     * @param options    consumer that configures {@link BitWriteOptions}
     * @return builder for continued chaining
     */
    public T bitRemove(int byteOffset, int byteSize, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitRemove(byteOffset, byteSize, opts);
    }

    /**
     * Remove bytes with pre-built write options (write).
     *
     * @param byteOffset start of the range to remove
     * @param byteSize   number of bytes to remove
     * @param options    pre-built write options
     * @return builder for continued chaining
     */
    public T bitRemove(int byteOffset, int byteSize, BitWriteOptions options) {
        Operation op = BitOperation.remove(bitPolicy(options), binName, byteOffset, byteSize);
        return opBuilder.addOp(op);
    }

    /**
     * Overwrite {@code bitSize} bits at {@code bitOffset} with {@code value} (write).
     *
     * @param bitOffset starting bit index within the blob
     * @param bitSize   width of the field in bits
     * @param value     bits to write
     * @return builder for continued chaining
     */
    public T bitSet(int bitOffset, int bitSize, byte[] value) {
        Operation op = BitOperation.set(BitPolicy.Default, binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Overwrite bits with caller-supplied write options (write).
     *
     * @param bitOffset starting bit index within the blob
     * @param bitSize   width of the field in bits
     * @param value     bits to write
     * @param options   consumer that configures {@link BitWriteOptions}
     * @return builder for continued chaining
     */
    public T bitSet(int bitOffset, int bitSize, byte[] value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitSet(bitOffset, bitSize, value, opts);
    }

    /**
     * Overwrite bits with pre-built write options (write).
     *
     * @param bitOffset starting bit index within the blob
     * @param bitSize   width of the field in bits
     * @param value     bits to write
     * @param options   pre-built write options
     * @return builder for continued chaining
     */
    public T bitSet(int bitOffset, int bitSize, byte[] value, BitWriteOptions options) {
        Operation op = BitOperation.set(bitPolicy(options), binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise OR {@code value} into {@code bitSize} bits at {@code bitOffset} (write).
     *
     * @param bitOffset starting bit index
     * @param bitSize   field width in bits
     * @param value     right-hand side of the OR
     * @return builder for continued chaining
     */
    public T bitOr(int bitOffset, int bitSize, byte[] value) {
        Operation op = BitOperation.or(BitPolicy.Default, binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise OR with caller-supplied write options (write).
     */
    public T bitOr(int bitOffset, int bitSize, byte[] value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitOr(bitOffset, bitSize, value, opts);
    }

    /**
     * Bitwise OR with pre-built write options (write).
     */
    public T bitOr(int bitOffset, int bitSize, byte[] value, BitWriteOptions options) {
        Operation op = BitOperation.or(bitPolicy(options), binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise XOR {@code value} into {@code bitSize} bits at {@code bitOffset} (write).
     */
    public T bitXor(int bitOffset, int bitSize, byte[] value) {
        Operation op = BitOperation.xor(BitPolicy.Default, binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise XOR with caller-supplied write options (write).
     */
    public T bitXor(int bitOffset, int bitSize, byte[] value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitXor(bitOffset, bitSize, value, opts);
    }

    /**
     * Bitwise XOR with pre-built write options (write).
     */
    public T bitXor(int bitOffset, int bitSize, byte[] value, BitWriteOptions options) {
        Operation op = BitOperation.xor(bitPolicy(options), binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise AND {@code value} into {@code bitSize} bits at {@code bitOffset} (write).
     */
    public T bitAnd(int bitOffset, int bitSize, byte[] value) {
        Operation op = BitOperation.and(BitPolicy.Default, binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise AND with caller-supplied write options (write).
     */
    public T bitAnd(int bitOffset, int bitSize, byte[] value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitAnd(bitOffset, bitSize, value, opts);
    }

    /**
     * Bitwise AND with pre-built write options (write).
     */
    public T bitAnd(int bitOffset, int bitSize, byte[] value, BitWriteOptions options) {
        Operation op = BitOperation.and(bitPolicy(options), binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Invert every bit in the range {@code [bitOffset, bitOffset + bitSize)} (write).
     */
    public T bitNot(int bitOffset, int bitSize) {
        Operation op = BitOperation.not(BitPolicy.Default, binName, bitOffset, bitSize);
        return opBuilder.addOp(op);
    }

    /**
     * Bitwise NOT with caller-supplied write options (write).
     */
    public T bitNot(int bitOffset, int bitSize, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitNot(bitOffset, bitSize, opts);
    }

    /**
     * Bitwise NOT with pre-built write options (write).
     */
    public T bitNot(int bitOffset, int bitSize, BitWriteOptions options) {
        Operation op = BitOperation.not(bitPolicy(options), binName, bitOffset, bitSize);
        return opBuilder.addOp(op);
    }

    /**
     * Left-shift {@code bitSize} bits at {@code bitOffset} by {@code shift} bits (write).
     */
    public T bitLshift(int bitOffset, int bitSize, int shift) {
        Operation op = BitOperation.lshift(BitPolicy.Default, binName, bitOffset, bitSize, shift);
        return opBuilder.addOp(op);
    }

    /**
     * Left-shift with caller-supplied write options (write).
     */
    public T bitLshift(int bitOffset, int bitSize, int shift, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitLshift(bitOffset, bitSize, shift, opts);
    }

    /**
     * Left-shift with pre-built write options (write).
     */
    public T bitLshift(int bitOffset, int bitSize, int shift, BitWriteOptions options) {
        Operation op = BitOperation.lshift(bitPolicy(options), binName, bitOffset, bitSize, shift);
        return opBuilder.addOp(op);
    }

    /**
     * Right-shift {@code bitSize} bits at {@code bitOffset} by {@code shift} bits (write).
     */
    public T bitRshift(int bitOffset, int bitSize, int shift) {
        Operation op = BitOperation.rshift(BitPolicy.Default, binName, bitOffset, bitSize, shift);
        return opBuilder.addOp(op);
    }

    /**
     * Right-shift with caller-supplied write options (write).
     */
    public T bitRshift(int bitOffset, int bitSize, int shift, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitRshift(bitOffset, bitSize, shift, opts);
    }

    /**
     * Right-shift with pre-built write options (write).
     */
    public T bitRshift(int bitOffset, int bitSize, int shift, BitWriteOptions options) {
        Operation op = BitOperation.rshift(bitPolicy(options), binName, bitOffset, bitSize, shift);
        return opBuilder.addOp(op);
    }

    /**
     * Write integer {@code value} into {@code bitSize} bits at {@code bitOffset} (write).
     */
    public T bitSetInt(int bitOffset, int bitSize, long value) {
        Operation op = BitOperation.setInt(BitPolicy.Default, binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Write integer with caller-supplied write options (write).
     */
    public T bitSetInt(int bitOffset, int bitSize, long value, Consumer<BitWriteOptions> options) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitSetInt(bitOffset, bitSize, value, opts);
    }

    /**
     * Write integer with pre-built write options (write).
     */
    public T bitSetInt(int bitOffset, int bitSize, long value, BitWriteOptions options) {
        Operation op = BitOperation.setInt(bitPolicy(options), binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Add {@code value} to the unsigned integer in {@code bitSize} bits at {@code bitOffset} (write).
     *
     * <p>Overflow/underflow fails the operation. See {@link #bitAdd(int, int, long, boolean, BitOverflowAction)}
     * for signed and overflow control.</p>
     */
    public T bitAdd(int bitOffset, int bitSize, long value) {
        return bitAdd(bitOffset, bitSize, value, false, BitOverflowAction.FAIL);
    }

    /**
     * Add {@code value} to the integer in {@code bitSize} bits at {@code bitOffset} (write).
     */
    public T bitAdd(int bitOffset, int bitSize, long value, boolean signed, BitOverflowAction action) {
        Operation op = BitOperation.add(BitPolicy.Default, binName, bitOffset, bitSize, value, signed, action);
        return opBuilder.addOp(op);
    }

    /**
     * Add with caller-supplied write options (write).
     */
    public T bitAdd(
        int bitOffset,
        int bitSize,
        long value,
        boolean signed,
        BitOverflowAction action,
        Consumer<BitWriteOptions> options
    ) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitAdd(bitOffset, bitSize, value, signed, action, opts);
    }

    /**
     * Add with pre-built write options (write).
     */
    public T bitAdd(
        int bitOffset,
        int bitSize,
        long value,
        boolean signed,
        BitOverflowAction action,
        BitWriteOptions options
    ) {
        Operation op = BitOperation.add(bitPolicy(options), binName, bitOffset, bitSize, value, signed, action);
        return opBuilder.addOp(op);
    }

    /**
     * Subtract {@code value} from the unsigned integer in {@code bitSize} bits at {@code bitOffset} (write).
     *
     * <p>Overflow/underflow fails the operation. See
     * {@link #bitSubtract(int, int, long, boolean, BitOverflowAction)} for signed and overflow control.</p>
     */
    public T bitSubtract(int bitOffset, int bitSize, long value) {
        return bitSubtract(bitOffset, bitSize, value, false, BitOverflowAction.FAIL);
    }

    /**
     * Subtract {@code value} from the integer in {@code bitSize} bits at {@code bitOffset} (write).
     */
    public T bitSubtract(int bitOffset, int bitSize, long value, boolean signed, BitOverflowAction action) {
        Operation op = BitOperation.subtract(BitPolicy.Default, binName, bitOffset, bitSize, value, signed, action);
        return opBuilder.addOp(op);
    }

    /**
     * Subtract with caller-supplied write options (write).
     */
    public T bitSubtract(
        int bitOffset,
        int bitSize,
        long value,
        boolean signed,
        BitOverflowAction action,
        Consumer<BitWriteOptions> options
    ) {
        BitWriteOptions opts = new BitWriteOptions();
        options.accept(opts);
        return bitSubtract(bitOffset, bitSize, value, signed, action, opts);
    }

    /**
     * Subtract with pre-built write options (write).
     */
    public T bitSubtract(
        int bitOffset,
        int bitSize,
        long value,
        boolean signed,
        BitOverflowAction action,
        BitWriteOptions options
    ) {
        Operation op = BitOperation.subtract(bitPolicy(options), binName, bitOffset, bitSize, value, signed, action);
        return opBuilder.addOp(op);
    }

    /**
     * Read {@code bitSize} bits at {@code bitOffset} as raw bytes (read).
     *
     * @param bitOffset starting bit index
     * @param bitSize   number of bits to read
     * @return builder for continued chaining
     */
    public T bitGet(int bitOffset, int bitSize) {
        Operation op = BitOperation.get(binName, bitOffset, bitSize);
        return opBuilder.addOp(op);
    }

    /**
     * Count bits set to {@code 1} in the given range (read).
     *
     * @param bitOffset starting bit index
     * @param bitSize   number of bits to scan
     * @return builder for continued chaining
     */
    public T bitCount(int bitOffset, int bitSize) {
        Operation op = BitOperation.count(binName, bitOffset, bitSize);
        return opBuilder.addOp(op);
    }

    /**
     * Scan from the left for the first bit matching {@code value} (read).
     *
     * @param bitOffset starting bit index
     * @param bitSize   number of bits to scan
     * @param value     {@code true} to find a set bit, {@code false} for unset
     * @return builder for continued chaining
     */
    public T bitLscan(int bitOffset, int bitSize, boolean value) {
        Operation op = BitOperation.lscan(binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Scan from the right for the first bit matching {@code value} (read).
     *
     * @param bitOffset starting bit index
     * @param bitSize   number of bits to scan
     * @param value     {@code true} to find a set bit, {@code false} for unset
     * @return builder for continued chaining
     */
    public T bitRscan(int bitOffset, int bitSize, boolean value) {
        Operation op = BitOperation.rscan(binName, bitOffset, bitSize, value);
        return opBuilder.addOp(op);
    }

    /**
     * Decode an integer from {@code bitSize} bits at {@code bitOffset} (read).
     *
     * @param bitOffset starting bit index
     * @param bitSize   width of the integer in bits
     * @param signed    {@code true} to interpret as two's-complement signed
     * @return builder for continued chaining
     */
    public T bitGetInt(int bitOffset, int bitSize, boolean signed) {
        Operation op = BitOperation.getInt(binName, bitOffset, bitSize, signed);
        return opBuilder.addOp(op);
    }

    private static BitPolicy bitPolicy(BitWriteOptions options) {
        return new BitPolicy(options.toFlags());
    }

    // ----------------------------------------
    // GeoJSON
    // ----------------------------------------

    /**
     * Queues a write that sets this bin to a GeoJSONValue.
     *
     * @param value GeoJSON value to store
     * @return the parent operation builder for chaining
     */
    public T setTo(Value.GeoJSONValue value) {
        return opBuilder.setTo(new Bin(binName, value));
    }

    /**
     * Queues a write that sets this bin to a GeoJSONValue.
     *
     * @param value GeoJSON string to store
     * @return the parent operation builder for chaining
     */
    public T setToGeoJson(String value) {
        return opBuilder.setTo(Bin.asGeoJSON(binName, value));
    }
}
