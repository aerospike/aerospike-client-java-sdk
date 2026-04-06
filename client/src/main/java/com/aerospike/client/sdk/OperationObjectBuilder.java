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

import com.aerospike.client.sdk.ael.BooleanExpression;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.query.PreparedDsl;
import com.aerospike.client.sdk.query.WhereClauseProcessor;

public class OperationObjectBuilder<T> extends AbstractFilterableBuilder implements FilterableOperation<OperationObjectBuilder<T>> {
    private final DataSet dataSet;
    //private final List<Operation> ops = new ArrayList<>();
    private final OpType opType;
    private final Session session;

    /**
     * Constructs a new OperationObjectBuilder for performing operations on objects.
     *
     * @param session the session to use for database operations
     * @param dataSet the dataset on which to perform operations
     * @param type the type of operation to perform
     */
    public OperationObjectBuilder(Session session, DataSet dataSet, OpType type) {
        this.dataSet = dataSet;
        this.opType = type;
        this.session = session;
    }

    /**
     * {@inheritDoc}
     * 
     * <p>Overridden to ensure UPDATE and REPLACE_IF_EXISTS operations always include
     * KEY_NOT_FOUND_ERROR results, since these operations are expected to fail if the
     * record doesn't exist.</p>
     */
    @Override
    public boolean shouldIncludeResult(int resultCode) {
        if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR) {
            // UPDATE and REPLACE_IF_EXISTS must report KEY_NOT_FOUND_ERROR because
            // these operations are semantically expected to fail on non-existent records.
            return includeMissingKeys || opType == OpType.UPDATE || opType == OpType.REPLACE_IF_EXISTS;
        }
        return super.shouldIncludeResult(resultCode);
    }

    /**
     * Specifies a list of objects to operate on.
     *
     * @param elements the list of objects to operate on
     * @return an ObjectBuilder for configuring and executing the operation
     */
    public ObjectBuilder<T> objects(List<T> elements) {
        return new ObjectBuilder<>(this, elements);
    }

    /**
     * Specifies multiple objects to operate on using varargs.
     *
     * @param element1 the first object to operate on
     * @param element2 the second object to operate on
     * @param elements additional objects to operate on (varargs)
     * @return an ObjectBuilder for configuring and executing the operation
     */
    @SuppressWarnings("unchecked")
	public ObjectBuilder<T> objects(T element1, T element2, T ... elements) {
        List<T> elementList = new ArrayList<>();
        elementList.add(element1);
        elementList.add(element2);
        for (T thisElement : elements) {
            elementList.add(thisElement);
        }
        return new ObjectBuilder<>(this, elementList);
    }

    /**
     * Specifies a single object to operate on.
     *
     * @param element the object to operate on
     * @return an ObjectBuilder for configuring and executing the operation
     */
    public ObjectBuilder<T> object(T element) {
        return new ObjectBuilder<T>(this, element);
    }

    /**
     * Specify a set of bin names for the bins/id/values pattern.
     * Follow this with {@code .id(x).values(...)} pairs to define each record.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * session.insert(customerDataSet)
     *     .bins("name", "age")
     *     .id(900).values("Tim", 312)
     *     .id(901).values("Jane", 28)
     *     .id(902).values("Bob", 54)
     *     .execute();
     * }</pre>
     *
     * @param binName the first bin name (required)
     * @param binNames additional bin names
     * @return IdValuesBuilder for specifying id/values rows
     */
    public IdValuesBuilder bins(String binName, String... binNames) {
        return new IdValuesBuilder(session, dataSet, opType, binName, binNames);
    }

    /**
     * Gets the dataset associated with this builder.
     *
     * @return the dataset
     */
    public DataSet getDataSet() {
        return dataSet;
    }

    /**
     * Gets the session associated with this builder.
     *
     * @return the session
     */
    public Session getSession() {
        return session;
    }

    /**
     * Gets the operation type for this builder.
     *
     * @return the operation type
     */
    public OpType getOpType() {
        return opType;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> where(String dsl, Object ... params) {
        setWhereClause(createWhereClauseProcessor(false, dsl, params));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> where(Expression e) {
        setWhereClause(WhereClauseProcessor.from(e));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationObjectBuilder<T> includeMissingKeys() {
        this.includeMissingKeys = true;
        return this;
    }

    /**
     * Gets the where clause processor for this builder.
     *
     * @return the where clause processor, or null if no where clause has been set
     */
    public WhereClauseProcessor getDsl() {
        return dsl;
    }

    /**
     * Checks whether the includeMissingKeys flag is set.
     *
     * @return true if includeMissingKeys is enabled, false otherwise
     */
    public boolean isIncludeMissingKeys() {
        return includeMissingKeys;
    }

    /**
     * Checks whether the failOnFilteredOut flag is set.
     *
     * @return true if failOnFilteredOut is enabled, false otherwise
     */
    public boolean isFailOnFilteredOut() {
        return failOnFilteredOut;
    }
}
