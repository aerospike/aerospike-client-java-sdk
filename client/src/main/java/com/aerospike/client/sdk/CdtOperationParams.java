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

import com.aerospike.client.sdk.CdtGetOrRemoveBuilder.CdtOperation;
import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;
import com.aerospike.client.sdk.exp.Exp;

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

    /** Filter expression for {@link CdtOperation#ALL_CHILDREN_WITH_FILTER}; cleared after push. */
    private Exp pathChildFilterExp;

    /**
     * Number of {@code onEachChild} segments along this path (including an initial segment at the bin root when
     * created via {@link #forEachChildAtBinRoot()}).
     */
    private int eachChildSegmentCount;

    private CdtOperationParams() {
    }

    /**
     * Begin a nested path at the top-level bin with {@link CTX#allChildren()} (iterate every child of the bin value).
     */
    public static CdtOperationParams forEachChildAtBinRoot() {
        CdtOperationParams p = new CdtOperationParams();
        p.operation = CdtOperation.ALL_CHILDREN;
        p.eachChildSegmentCount = 1;
        return p;
    }

    /**
     * Same as {@link #forEachChildAtBinRoot()} with a predicate on each child.
     */
    public static CdtOperationParams forEachChildAtBinRootWithFilter(Exp filter) {
        if (filter == null) {
            throw new NullPointerException("filter");
        }
        CdtOperationParams p = new CdtOperationParams();
        p.operation = CdtOperation.ALL_CHILDREN_WITH_FILTER;
        p.pathChildFilterExp = filter;
        p.eachChildSegmentCount = 1;
        return p;
    }

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
        this.pad = pad;
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

    public int getEachChildSegmentCount() {
        return eachChildSegmentCount;
    }

    /**
     * Pushes the current selection, then sets {@link CdtOperation#ALL_CHILDREN} for the next path segment.
     */
    public void pushCurrentToContextAndReplaceWithAllChildren() {
        pushCurrentToContext();
        this.operation = CdtOperation.ALL_CHILDREN;
        this.pathChildFilterExp = null;
        eachChildSegmentCount++;
    }

    /**
     * Pushes the current selection, then sets {@link CdtOperation#ALL_CHILDREN_WITH_FILTER} for the next path segment.
     */
    public void pushCurrentToContextAndReplaceWithAllChildrenWithFilter(Exp filter) {
        if (filter == null) {
            throw new NullPointerException("filter");
        }
        pushCurrentToContext();
        this.operation = CdtOperation.ALL_CHILDREN_WITH_FILTER;
        this.pathChildFilterExp = filter;
        eachChildSegmentCount++;
    }

    /**
     * Flushes the final path segment and returns the full {@link CTX} array for {@code selectByPath} /
     * {@code modifyByPath}. Requires at least one {@code onEachChild} segment ({@link #getEachChildSegmentCount()}).
     */
    public CTX[] finishContextPathForPathExpression() {
        if (eachChildSegmentCount < 1) {
            throw new IllegalStateException(
                    "Path selection (collect*), modifyBy, or removeMatches requires at least one onEachChild() in the path.");
        }
        pushCurrentToContext();
        CTX[] out = context();
        return out != null ? out : new CTX[0];
    }

    private CTX currentToCtx() {
        switch (operation) {
        case ALL_CHILDREN:
            return CTX.allChildren();
        case ALL_CHILDREN_WITH_FILTER:
            if (pathChildFilterExp == null) {
                throw new IllegalStateException("ALL_CHILDREN_WITH_FILTER requires a filter expression");
            }
            return CTX.allChildrenWithFilter(pathChildFilterExp);
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
     *
     * @param operation
     * @param int1
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
    }

    /**
     * Pushes the current selection onto the context path, then sets the next operation and two integer arguments.
     *
     * @param operation next selection discriminator
     * @param int1      first integer argument
     * @param int2      second integer argument
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1, int int2) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
        this.int2 = int2;
    }

    /**
     * Pushes the current selection onto the context path, then sets list-by-index state with creation policy.
     *
     * @param operation   expected to be list index selection with create semantics
     * @param int1        list index
     * @param createType  list order if the list is created
     * @param pad         whether to pad when creating
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, int int1, ListOrder createType, boolean pad) {
        pushCurrentToContext();
        this.operation = operation;
        this.int1 = int1;
        this.listCreateType = createType;
        this.pad = pad;
    }

    /**
     * Pushes the current selection onto the context path, then sets the next operation and one value.
     *
     * @param operation next selection discriminator
     * @param val1      value argument for the new selection
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
    }

    /**
     * Pushes the current selection onto the context path, then sets map-by-key selection with map creation order.
     *
     * @param operation  next selection discriminator
     * @param val1       map key
     * @param createType map order if the map must be created
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, MapOrder createType) {
        pushCurrentToContext();
        this.operation = operation;
        this.mapCreateType = createType;
        this.val1 = val1;
    }

    /**
     * Pushes the current selection onto the context path, then sets an operation with two value bounds.
     *
     * @param operation range-style selection
     * @param val1      first bound
     * @param val2      second bound
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, Value val2) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.val2 = val2;
    }

    /**
     * Pushes the current selection onto the context path, then sets an operation with a value and one integer.
     *
     * @param operation combined value and int selection
     * @param val1      reference value or key
     * @param int1      integer argument (e.g. relative index or rank)
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, int int1) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.int1 = int1;
    }

    /**
     * Pushes the current selection onto the context path, then sets an operation with a value and two integers.
     *
     * @param operation combined selection
     * @param val1      reference value or key
     * @param int1      first integer argument
     * @param int2      second integer argument (e.g. count)
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, Value val1, int int1, int int2) {
        pushCurrentToContext();
        this.operation = operation;
        this.val1 = val1;
        this.int1 = int1;
        this.int2 = int2;
    }

    /**
     * Pushes the current selection onto the context path, then sets a multi-value operation.
     *
     * @param operation key-list, value-list, or similar
     * @param values    Aerospike values for the new selection
     */
    public void pushCurrentToContextAndReplaceWith(CdtOperation operation, List<Value> values) {
        pushCurrentToContext();
        this.operation = operation;
        this.values = values;
    }

    /**
     * Appends a {@link CTX} derived from the current {@link #getOperation()} and arguments to the nested path,
     * then clears map and list creation hints for the next step.
     */
    public void pushCurrentToContext() {
        if (ctx == null) {
            ctx = new ArrayList<>();
        }
        ctx.add(currentToCtx());
        if (operation == CdtOperation.ALL_CHILDREN_WITH_FILTER) {
            pathChildFilterExp = null;
        }
        mapCreateType = null;
        listCreateType = null;
    }

    /**
     * @return CDT context array for the server API, or {@code null} if no path has been built
     */
    public CTX[] context() {
        if (this.ctx == null) {
            return null;
        }
        return this.ctx.toArray(CTX[]::new);
    }

    /**
     * @return {@code true} if {@link #getInt2()} is non-zero (used to distinguish optional second int)
     */
    public boolean hasInt2() {
        return this.int2 != 0;
    }

}