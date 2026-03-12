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
package com.aerospike.client.fluent;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.fluent.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.fluent.cdt.CTX;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.cdt.MapOrder;

public class CdtOperationParams {
    private CdtOperation operation;
    private List<CTX> ctx;
    private int int1;
    private int int2;
    private int int3;
    private Value val1;
    private Value val2;
    private List<Value> values;
    private MapOrder mapCreateType;
    private ListOrder listCreateType;
    private boolean pad;

    public CdtOperationParams(CdtOperation operation, int int1) {
        this.int1 = int1;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, int int1, int int2) {
        this.int1 = int1;
        this.int2 = int2;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, int int1, ListOrder createType, boolean pad) {
        this.int1 = int1;
        this.operation = operation;
        this.listCreateType = createType;
    }
    public CdtOperationParams(CdtOperation operation, Value val1) {
        this.val1 = val1;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, Value val1, MapOrder createType) {
        this.val1 = val1;
        this.operation = operation;
        this.mapCreateType = createType;
    }
    public CdtOperationParams(CdtOperation operation, Value val1, Value val2) {
        this.val1 = val1;
        this.val2 = val2;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, Value val1, int int1) {
        this.val1 = val1;
        this.int1 = int1;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, Value val1, int int1, int int2) {
        this.val1 = val1;
        this.int1 = int1;
        this.int2 = int2;
        this.operation = operation;
    }
    public CdtOperationParams(CdtOperation operation, List<Value> values) {
        this.values = values;
        this.operation = operation;
    }

    public List<CTX> getCtx() {
        return ctx;
    }
    public int getInt1() {
        return int1;
    }
    public int getInt2() {
        return int2;
    }
    public int getInt3() {
        return int3;
    }
    public Value getVal1() {
        return val1;
    }
    public Value getVal2() {
        return val2;
    }
    public CdtOperation getOperation() {
        return operation;
    }
    public List<Value> getValues() {
        return values;
    }
    public boolean isPad() {
        return pad;
    }
    public ListOrder getListCreateType() {
        return listCreateType;
    }
    public MapOrder getMapCreateType() {
        return mapCreateType;
    }

    private CTX currentToCtx() {
        switch (operation) {
        case MAP_BY_INDEX:
            return CTX.mapIndex(int1);
        case MAP_BY_KEY:
            if (mapCreateType == null) {
                return CTX.mapKey(val1);
            }
            else {
                return CTX.mapKeyCreate(val1, mapCreateType);
            }
        case MAP_BY_RANK:
            return CTX.mapRank(int1);
        case MAP_BY_VALUE:
            return CTX.mapValue(val1);
        case LIST_BY_INDEX:
            if (listCreateType == null) {
                return CTX.listIndex(int1);
            }
            else {
                return CTX.listIndexCreate(int1, listCreateType, pad);
            }
        case LIST_BY_RANK:
            return CTX.listRank(int1);
        case LIST_BY_VALUE:
            return CTX.listValue(val1);
        default:
            throw new IllegalStateException("Cannot call 'currentToCtx' with an operation of " + operation);
        }
    }
    /**
     * Some elements can be part of a path, part of a context or even the terminal selector. This
     * method takes the current values in the params, makes them into a CTX, adds this CTX to the CTX path
     * and replaces the values in this class with the new values.
     * @param operation
     * @param int1
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
    }
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1, int int2) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
        this.int2 = int2;
    }
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1, ListOrder createType, boolean pad) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
        this.listCreateType = createType;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, MapOrder createType) {
        pushCurrentToContext();
        this.operation = operation;
        this.mapCreateType = createType;
        this.val1 = val1;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, Value val2) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.val2 = val2;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, int int1) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.int1 = int1;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, int int1, int int2) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.int1 = int1;
        this.int2 = int2;
    }

    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, List<Value> values) {
        pushCurrentToContext();
        this.operation = operation;
        this.values = values;
    }

    public void pushCurrentToContext() {
        if (ctx == null) {
            ctx = new ArrayList<>();
        }
        ctx.add(currentToCtx());
        mapCreateType = null;
        listCreateType = null;
    }

    public CTX[] context() {
        if (this.ctx == null) {
            return null;
        }
        return this.ctx.toArray(CTX[]::new);
    }

    public boolean hasInt2() {
        return this.int2 != 0;
    }

}