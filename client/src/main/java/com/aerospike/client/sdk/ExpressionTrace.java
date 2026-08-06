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

import java.io.Serializable;
import java.util.Arrays;

/**
 * Structured expression build/eval trace surfaced at {@link ErrorDetailVerbosity#EXPRESSION_TRACE}.
 */
public class ExpressionTrace implements Serializable {
    private static final long serialVersionUID = 1L;

    //-------------------------------------------------------
    // Wire constants (mirror server proto.h).
    //-------------------------------------------------------

    /** Top-level field-45 error-detail key carrying the nested expression-trace map. */
    public static final int AS_ERROR_DETAIL_KEY_EXP_TRACE = 3;

    /** Nested trace key: phase (uint; {@link #PHASE_BUILD} / {@link #PHASE_EVAL}). */
    public static final int KEY_PHASE = 1;
    /** Nested trace key: byte_offset into the msgpack expression payload (uint). */
    public static final int KEY_BYTE_OFFSET = 2;
    /** Nested trace key: failing op name (str). */
    public static final int KEY_OP = 3;
    /** Nested trace key: true nesting depth of the fault (uint). */
    public static final int KEY_DEPTH = 4;
    /** Nested trace key: op-name chain root&rarr;fault (array of str). */
    public static final int KEY_PATH = 5;
    /** Nested trace key: human-only rendered snippet of the failing element (str). */
    public static final int KEY_SNIPPET = 6;
    /** Nested trace key: eval-phase outcome (uint; reserved, SERVER-1138). */
    public static final int KEY_OUTCOME = 7;
    /** Nested trace key: source language (uint; {@link #LANG_MSGPACK} / {@link #LANG_AEL}). */
    public static final int KEY_LANG = 8;
    /** Nested trace key: char offset into AEL source text (uint; reserved). */
    public static final int KEY_AEL_OFFSET = 9;
    /** Nested trace key: byte width of the offending AEL source region (uint; reserved). */
    public static final int KEY_AEL_SPAN = 10;
    /** Nested trace key: 1-based line in AEL source (uint; reserved). */
    public static final int KEY_AEL_LINE = 11;
    /** Nested trace key: decisive comparison operands {@code [lhs, rhs]} (str[2]). */
    public static final int KEY_OPERANDS = 13;
    /** Nested trace key: 1-based column in AEL source (uint; reserved). */
    public static final int KEY_AEL_COL = 12;

    /** Phase value: expression build failed. */
    public static final int PHASE_BUILD = 1;
    /** Phase value: expression evaluation failed (reserved, SERVER-1138). */
    public static final int PHASE_EVAL = 2;

    /** Source language: msgpack (the implied default when {@code lang} is absent). */
    public static final int LANG_MSGPACK = 1;
    /** Source language: AEL DSL (reserved for a future server branch). */
    public static final int LANG_AEL = 2;

    /**
     * The {@code "..."} sentinel the server splices into {@link #getPath()} when the
     * true nesting depth exceeds the path-frame cap. {@link #getDepth()} still reports
     * the true count.
     */
    public static final String PATH_TRUNCATION_SENTINEL = "...";

    //-------------------------------------------------------
    // Fields (all optional; sentinels mark "absent").
    //-------------------------------------------------------

    private final int phase;
    private final int byteOffset;
    private final String op;
    private final int depth;
    private final String[] path;
    private final String snippet;
    private final int lang;
    private final int aelOffset;
    private final int aelSpan;
    private final String lhsOperand;
    private final String rhsOperand;

    /**
     * Construct a trace. Use {@code -1} / {@code null} for any absent field.
     *
     * @param phase      {@link #PHASE_BUILD} / {@link #PHASE_EVAL}, or {@code -1} if absent
     * @param byteOffset byte offset into the msgpack expression payload, or {@code -1}
     * @param op         failing op name, or {@code null}
     * @param depth      true nesting depth of the fault, or {@code -1}
     * @param path       op-name chain root&rarr;fault, or {@code null}
     * @param snippet    rendered snippet of the failing element, or {@code null}
     * @param lang       {@link #LANG_MSGPACK} / {@link #LANG_AEL}, or {@code -1} (&rArr; msgpack)
     * @param aelOffset  char offset into AEL source text, or {@code -1}
     * @param aelSpan    byte width of the offending AEL source region, or {@code -1}
     * @param lhsOperand left-hand operand of the decisive comparison, or {@code null}
     * @param rhsOperand right-hand operand of the decisive comparison, or {@code null}
     */
    public ExpressionTrace(int phase, int byteOffset, String op, int depth, String[] path,
        String snippet, int lang, int aelOffset, int aelSpan, String lhsOperand, String rhsOperand) {
        this.phase = phase;
        this.byteOffset = byteOffset;
        this.op = op;
        this.depth = depth;
        this.path = path;
        this.snippet = snippet;
        this.lang = lang;
        this.aelOffset = aelOffset;
        this.aelSpan = aelSpan;
        this.lhsOperand = lhsOperand;
        this.rhsOperand = rhsOperand;
    }

    /**
     * Phase that failed: {@link #PHASE_BUILD} or {@link #PHASE_EVAL}. Returns {@code -1}
     * when absent. Today the server emits build traces only ({@code PHASE_BUILD}).
     */
    public int getPhase() {
        return phase;
    }

    /**
     * Byte offset into the msgpack expression payload of the failing element, or
     * {@code -1} when absent. This is a coordinate into the wire payload the client
     * sent — not into AEL source text (see {@link #getAelOffset()}).
     */
    public int getByteOffset() {
        return byteOffset;
    }

    /**
     * Failing op name (pre-rendered server-side), or {@code null} when absent.
     */
    public String getOp() {
        return op;
    }

    /**
     * True nesting depth of the fault, or {@code -1} when absent. Reports the true
     * count even when {@link #getPath()} was truncated to the frame cap.
     */
    public int getDepth() {
        return depth;
    }

    /**
     * Op-name chain from root to fault, or {@code null} when absent. May contain a
     * {@link #PATH_TRUNCATION_SENTINEL} ({@code "..."}) element mid-array when the true
     * nesting exceeded the server's path-frame cap; {@link #getDepth()} still reports
     * the true count.
     */
    public String[] getPath() {
        return path;
    }

    /**
     * Human-only rendered snippet of the failing element, or {@code null} when absent
     * (it is the first field the server drops under a tight byte budget).
     */
    public String getSnippet() {
        return snippet;
    }

    /**
     * Source language: {@link #LANG_MSGPACK} or {@link #LANG_AEL}. An absent {@code lang}
     * key means msgpack (the default), so this returns {@link #LANG_MSGPACK} when the
     * server omitted it.
     */
    public int getLang() {
        return (lang < 0) ? LANG_MSGPACK : lang;
    }

    /**
     * Char offset into the AEL source text, or {@code -1} when absent. Reserved for the
     * AEL DSL branch; absent on today's msgpack build traces. A different coordinate
     * space from {@link #getByteOffset()}.
     */
    public int getAelOffset() {
        return aelOffset;
    }

    /**
     * Byte width of the offending AEL source region, or {@code -1} when absent.
     * Reserved for the AEL DSL branch.
     */
    public int getAelSpan() {
        return aelSpan;
    }

    /**
     * Left-hand operand of the decisive comparison in a filter-decision explainer trace,
     * or {@code null} when absent (dropped under a tight byte budget or not applicable).
     */
    public String getLhsOperand() {
        return lhsOperand;
    }

    /**
     * Right-hand operand of the decisive comparison in a filter-decision explainer trace,
     * or {@code null} when absent.
     */
    public String getRhsOperand() {
        return rhsOperand;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        toString(sb);
        return sb.toString();
    }

    public void toString(StringBuilder sb) {
        sb.append("ExpressionTrace[phase=").append(phase);
        sb.append(", byteOffset=").append(byteOffset);
        if (op != null) {
            sb.append(", op=").append(op);
        }
        sb.append(", depth=").append(depth);
        if (path != null) {
            sb.append(", path=").append(Arrays.toString(path));
        }
        if (snippet != null) {
            sb.append(", snippet=").append(snippet);
        }
        sb.append(", lang=").append(getLang());
        if (aelOffset >= 0) {
            sb.append(", aelOffset=").append(aelOffset);
        }
        if (aelSpan >= 0) {
            sb.append(", aelSpan=").append(aelSpan);
        }
        if (lhsOperand != null) {
            sb.append(", lhs=").append(lhsOperand);
        }
        if (rhsOperand != null) {
            sb.append(", rhs=").append(rhsOperand);
        }
        sb.append(']');
    }
}
