/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.client.sdk.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;

/**
 * Server-backed AEL tests for a loop variable used as a <em>method receiver</em> inside a
 * filter — {@code @.strlen()}, {@code @key.upper()}, {@code @index.toString()}.
 *
 * <p>The grammar for this landed after the wildcard tests were written, so nothing else
 * covers it. Two rules govern which spellings work, and neither is guessable from the
 * others.
 *
 * <p><b>1. A type pin on a loop variable cannot be followed by a method.</b>
 * {@code @:LIST.count()} is a parse error; {@code (@:LIST).count()} is fine. The parser
 * has no {@code loop_var . method_fn} production — the three receiver rules are spelled
 * with the bare token ({@code method_call ::= TOK_AT TOK_DOT method_fn}) because reducing
 * {@code loop_var} first would need the parse decision a token earlier than the lookahead
 * allows, and costs a conflict. Parentheses reach the method through
 * {@code exp_base ::= ( expr )} instead. This is a deliberate trade-off rather than a
 * defect, but the failing spelling is the natural one, so it is pinned here.
 *
 * <p><b>2. An unpinned loop variable only works for methods that resolve from the
 * particle at runtime.</b> String methods do ({@code @.strlen()} works); {@code .count()}
 * and the casts do not, because they need the receiver's type at compile time. So
 * {@code @.count()} fails while {@code (@:LIST).count()} succeeds — the parentheses are
 * not what fixes it, the pin is.
 *
 * <p>Fixture — one record:
 * <pre>
 *   ll     [[1, 2, 3], [4, 5]]                   list of lists
 *   ml     {alpha: [1, 2, 3], beta: [4, 5]}      map of lists
 *   ints   [10, 20, 30]
 *   strs   ["abc", "de", "fghi"]
 * </pre>
 */
public class AelLoopVarMethodTest extends ClusterTest {
    private static final String KEY = "ael_loopvar_method";

    private Key key;

    @BeforeAll
    public static void requireAelServer() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> mapOfLists = new LinkedHashMap<>();
        mapOfLists.put("alpha", List.of(1, 2, 3));
        mapOfLists.put("beta", List.of(4, 5));

        session.upsert(key)
            .bin("ll").setTo(List.of(List.of(1, 2, 3), List.of(4, 5)))
            .bin("ml").setTo(mapOfLists)
            .bin("ints").setTo(List.of(10, 20, 30))
            .bin("strs").setTo(List.of("abc", "de", "fghi"))
            .execute();
    }

    // --- bare loop variable as receiver ---

    /** String methods resolve from the particle, so the value needs no pin. */
    @Test
    public void bareValueLoopVarTakesStringMethods() {
        assertEquals(1L, selectLong("$.strs:LIST.*[?(@.strlen() == 3)].count()"));
        assertEquals(1L, selectLong("$.strs:LIST.*[?(@.upper() == 'DE')].count()"));
    }

    /** Parenthesising an unpinned value receiver changes nothing. */
    @Test
    public void parenthesesAloneDoNotChangeBareReceiver() {
        assertEquals(1L, selectLong("$.strs:LIST.*[?((@).strlen() == 3)].count()"));
    }

    /** {@code @key} is a string, and {@code @index} an int, so both carry methods bare. */
    @Test
    public void bareKeyAndIndexLoopVarsTakeMethods() {
        assertEquals(1L, selectLong("$.ml:MAP.*[?(@key.strlen() > 4)].count()"));
        assertEquals(1L, selectLong("$.ml:MAP.*[?(@key.upper() == 'ALPHA')].count()"));
        assertEquals(1L, selectLong("$.ints:LIST.*[?(@index.toString() == '1')].count()"));
    }

    // --- pinned loop variable: parentheses required ---

    /** The pin supplies the compile-time type that {@code .count()} and casts need. */
    @Test
    public void pinnedLoopVarInParenthesesTakesAnyMethod() {
        assertEquals(1L, selectLong("$.ll:LIST.*[?((@:LIST).count() > 2)].count()"));
        assertEquals(1L, selectLong("$.ml:MAP.*[?((@:LIST).count() > 2)].count()"));
        assertEquals(1L, selectLong("$.ints:LIST.*[?((@:INT).toString() == '20')].count()"));
        assertEquals(1L, selectLong("$.strs:LIST.*[?((@:STRING).strlen() == 3)].count()"));
        assertEquals(1L, selectLong("$.strs:LIST.*[?((@:STRING).upper() == 'DE')].count()"));
    }

    /**
     * Without the parentheses there is no production to reduce, uniformly for all three
     * loop variables. Dropping them is the natural spelling, so this is the mistake a
     * caller is most likely to make.
     */
    @Test
    public void pinnedLoopVarWithoutParenthesesIsRejected() {
        assertRejected("$.ll:LIST.*[?(@:LIST.count() > 2)].count()");
        assertRejected("$.strs:LIST.*[?(@:STRING.strlen() == 3)].count()");
        assertRejected("$.ml:MAP.*[?(@key:STRING.strlen() > 4)].count()");
        assertRejected("$.ints:LIST.*[?(@index:INT.toString() == '1')].count()");
    }

    // --- unpinned receiver: which methods are out of reach ---

    /**
     * {@code .count()} needs the receiver's container type when the op is built, which an
     * unpinned loop variable does not carry — parentheses do not help, only the pin does.
     */
    @Test
    public void unpinnedLoopVarCannotTakeCount() {
        assertRejected("$.ll:LIST.*[?(@.count() > 2)].count()");
        assertRejected("$.ll:LIST.*[?((@).count() > 2)].count()");
        assertRejected("$.ml:MAP.*[?(@.count() > 2)].count()");
    }

    /**
     * Casts parse on an unpinned receiver but fail while running, so this one surfaces as
     * a different result code than the rejections above.
     */
    @Test
    public void unpinnedLoopVarCastIsNotApplicable() {
        AerospikeException ex = assertThrows(AerospikeException.class,
            () -> selectValue("$.ints:LIST.*[?(@.toString() == '20')].count()"));
        assertEquals(ResultCode.OP_NOT_APPLICABLE, ex.getResultCode(),
            "unpinned cast should fail at evaluation, not parse");
    }

    // --- helpers ---

    private void assertRejected(String ael) {
        AerospikeException ex = assertThrows(AerospikeException.class, () -> selectValue(ael),
            () -> "expected server to reject AEL: " + ael);
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode(),
            () -> "unexpected result code for AEL: " + ael);
    }

    private long selectLong(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(Number.class, value, () -> "expected number for AEL: " + ael);
        return ((Number) value).longValue();
    }

    private Object selectValue(String ael) {
        try (RecordStream rs = session.query(key).bin("out").selectFrom(ael).execute()) {
            if (!rs.hasNext()) {
                return null;
            }
            return rs.next().recordOrThrow().getValue("out");
        }
    }
}
