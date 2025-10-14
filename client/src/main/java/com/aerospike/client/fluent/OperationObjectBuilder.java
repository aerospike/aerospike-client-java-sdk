package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.dsl.BooleanExpression;
import com.aerospike.client.fluent.exp.Exp;
import com.aerospike.client.fluent.query.PreparedDsl;
import com.aerospike.client.fluent.query.WhereClauseProcessor;

public class OperationObjectBuilder<T> implements FilterableOperation<OperationObjectBuilder<T>> {
    private final DataSet dataSet;
    private final List<Operation> ops = new ArrayList<>();
    private final OpType opType;
    private final Session session;
    protected WhereClauseProcessor dsl = null;
    protected boolean respondAllKeys = false;
    protected boolean failOnFilteredOut = false;

    public OperationObjectBuilder(Session session, DataSet dataSet, OpType type) {
        this.dataSet = dataSet;
        this.opType = type;
        this.session = session;
    }

    public ObjectBuilder<T> objects(List<T> elements) {
        return new ObjectBuilder<>(this, elements);
    }

    public ObjectBuilder<T> objects(T element1, T element2, T ... elements) {
        List<T> elementList = new ArrayList<>();
        elementList.add(element1);
        elementList.add(element2);
        for (T thisElement : elements) {
            elementList.add(thisElement);
        }
        return new ObjectBuilder<>(this, elementList);
    }

    public ObjectBuilder<T> object(T element) {
        return new ObjectBuilder<T>(this, element);
    }

    public DataSet getDataSet() {
        return dataSet;
    }
    public Session getSession() {
        return session;
    }

    public OpType getOpType() {
        return opType;
    }

    private void setWhereClause(WhereClauseProcessor clause) {
        if (this.dsl == null) {
            this.dsl = clause;
        }
        else {
            throw new IllegalArgumentException("Only one 'where' clause can be specified. There is already one of '%s' and another is being set to '%s'"
                    .formatted(this.dsl, clause));
        }
    }

    @Override
    public OperationObjectBuilder<T> where(String dsl, Object ... params) {
        WhereClauseProcessor impl;
        if (dsl == null || dsl.isEmpty()) {
            impl = null;
        }
        else if (params.length == 0) {
            impl = WhereClauseProcessor.from(false, dsl);
        }
        else {
            impl = WhereClauseProcessor.from(false, String.format(dsl, params));
        }
        setWhereClause(impl);
        return this;
    }

    @Override
    public OperationObjectBuilder<T> where(BooleanExpression dsl) {
        setWhereClause(WhereClauseProcessor.from(dsl));
        return this;
    }

    @Override
    public OperationObjectBuilder<T> where(PreparedDsl dsl, Object ... params) {
        setWhereClause(WhereClauseProcessor.from(false, dsl, params));
        return this;
    }

    @Override
    public OperationObjectBuilder<T> where(Exp exp) {
        setWhereClause(WhereClauseProcessor.from(exp));
        return this;
    }

    @Override
    public OperationObjectBuilder<T> failOnFilteredOut() {
        this.failOnFilteredOut = true;
        return this;
    }

    @Override
    public OperationObjectBuilder<T> respondAllKeys() {
        this.respondAllKeys = true;
        return this;
    }

    public WhereClauseProcessor getDsl() {
        return dsl;
    }

    public boolean isRespondAllKeys() {
        return respondAllKeys;
    }

    public boolean isFailOnFilteredOut() {
        return failOnFilteredOut;
    }
}
