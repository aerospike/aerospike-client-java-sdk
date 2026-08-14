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
package com.aerospike.client.sdk.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.ErrorDetailVerbosity;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * Compile-time surface of the AEL write flags: which {@code :PROPERTY} spellings the
 * server accepts, on which container, and which verb accepts which receiver.
 *
 * <p>Guards the spellings that were renamed or removed — list create-orders became
 * {@code SORTED} / {@code UNSORTED} / {@code UNSORTED_UNBOUND}, map literals narrowed to
 * {@code UNORDERED} alone, and {@code NO_OVERWRITE} / {@code NO_CREATE} were dropped in
 * favour of the {@code insert} / {@code update} verbs. Observable write behavior lives in
 * {@link AelPathWriteTest}.</p>
 */
public class AelWriteFlagSurfaceTest extends ClusterTest {
    private static final String KEY = "ael_write_flag_surface";
    private static final String BIN_MAP = "m";
    private static final String BIN_LIST = "l";

    private static Key key;
    /**
     * Gates the whole class on a build carrying the narrowed literal surface. That change
     * landed after the flag rename and the {@code NO_OVERWRITE} removal, so one probe
     * covers every diagnostic asserted here.
     */
    private static boolean serverHasNarrowedFlagSurface;

    @BeforeAll
    public static void setup() {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");

        key = args.set.id(KEY);
        session.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);

        session.upsert(key)
            .bin(BIN_MAP).setTo(map)
            .bin(BIN_LIST).setTo(List.of(100, 200, 300))
            .execute();

        serverHasNarrowedFlagSurface = probeSelectRejects(
            "{a: 1, b: 2}:KEY_ORDERED", "map literal's default");
    }

    // --- removed spellings: the verbs are now the only way to ask for these modes ---

    @Test
    public void noOverwriteFlagSpellingIsGone() {
        assertWriteRejected("$." + BIN_MAP + ":MAP.alpha.setTo(1):NO_OVERWRITE", "unknown property flag");
    }

    @Test
    public void noCreateFlagSpellingIsGone() {
        assertWriteRejected("$." + BIN_MAP + ":MAP.alpha.setTo(1):NO_CREATE", "unknown property flag");
    }

    // --- create-order spellings follow the container the selector already fixed ---

    @Test
    public void orderedIsNoLongerAFlagSpelling() {
        // Renamed to SORTED, so the resolver table no longer knows the word at all.
        assertWriteRejected("$." + BIN_LIST + ":LIST.[0]:ORDERED.setTo(1)", "unknown property flag");
    }

    @Test
    public void unorderedIsNotAListSegmentSpelling() {
        // UNSORTED and UNORDERED share one wire bit, so only the spelling separates them.
        assertWriteRejected("$." + BIN_LIST + ":LIST.[0]:UNORDERED.setTo(1)", "list segment order must be");
    }

    @Test
    public void unsortedIsNotAMapSegmentSpelling() {
        assertWriteRejected("$." + BIN_MAP + ":MAP.nested:UNSORTED.inner:INT.setTo(1)",
            "map segment order must be");
    }

    @Test
    public void createOrderAndTypePinOnOneSegmentConflict() {
        // The order names its container, so the pin would say the same thing twice.
        assertWriteRejected("$." + BIN_LIST + ":LIST.[9]:INT:UNSORTED_UNBOUND.setTo(1)",
            "drop the :type pin");
    }

    @Test
    public void unsortedUnboundIsASegmentPropertyNotAnOpFlag() {
        // setTo()'s own flag mask never carried a create-order; the leaf segment's
        // create bits are what the op picks up during finalization.
        assertWriteRejected("$." + BIN_LIST + ":LIST.[9].setTo(1):UNSORTED_UNBOUND",
            "property not valid for this node");
    }

    // --- map literals: :UNORDERED is the only order suffix ---
    //
    // A collection literal is an operand in its own right, so selecting one directly
    // is enough to compile it. A `${var}` bound to one cannot take a path suffix —
    // the grammar has no production for it.

    @Test
    public void mapLiteralKeyOrderedSuffixIsRedundant() {
        assertSelectRejected("{a: 1, b: 2}:KEY_ORDERED", "map literal's default");
    }

    @Test
    public void mapLiteralCannotBeKeyValueOrdered() {
        // A surface restriction, not an encoding limit — path segments still accept it.
        assertSelectRejected("{a: 1, b: 2}:KEY_VALUE_ORDERED", "cannot be key-value ordered");
    }

    @Test
    public void mapLiteralRejectsListOrderSpelling() {
        assertSelectRejected("{a: 1, b: 2}:SORTED", "only order suffix");
    }

    @Test
    public void mapLiteralUnorderedStillCompiles() {
        Object value = selectValue("{b: 2, a: 1}:UNORDERED");
        assertInstanceOf(Map.class, value);
        assertEquals(2, ((Map<?, ?>) value).size());
    }

    // --- list literals: SORTED / UNSORTED, and :SORTED is validated not sorted ---

    @Test
    public void listLiteralOrderedSuffixIsGone() {
        assertSelectRejected("[1, 2, 3]:ORDERED", "list literal order must be");
    }

    @Test
    public void listLiteralRejectsMapOrderSpelling() {
        assertSelectRejected("[1, 2, 3]:UNORDERED", "list literal order must be");
    }

    @Test
    public void sortedListLiteralMustAlreadyBeAscending() {
        // :SORTED asserts the literal is already ascending rather than sorting it.
        assertSelectRejected("[3, 1, 2]:SORTED", "ascending order");
    }

    @Test
    public void sortedListLiteralInAscendingOrderCompiles() {
        assertThat(selectList("[1, 2, 3]:SORTED"))
            .extracting(v -> ((Number) v).longValue())
            .containsExactly(1L, 2L, 3L);
    }

    @Test
    public void unsortedListLiteralCompiles() {
        assertThat(selectList("[3, 1, 2]:UNSORTED"))
            .extracting(v -> ((Number) v).longValue())
            .containsExactly(3L, 1L, 2L);
    }

    // --- verb receivers, resolved once the leaf fixes the container ---

    @Test
    public void updateItemsRequiresAMapReceiver() {
        assertWriteRejected("$." + BIN_LIST + ":LIST.updateItems([1, 2])", "map receiver");
    }

    @Test
    public void insertItemsOnAListRequiresAnIndexLeaf() {
        // On a map the same verb is whole-collection; a list needs the position.
        assertWriteRejected("$." + BIN_LIST + ":LIST.insertItems([1, 2])", "index leaf");
    }

    @Test
    public void addUniqueIsRejectedOnAMapWrite() {
        assertWriteRejected("$." + BIN_MAP + ":MAP.alpha.setTo(1):ADD_UNIQUE", "list-only");
    }

    // --- helpers ---

    private void assertWriteRejected(String ael, String messageFragment) {
        Assumptions.assumeTrue(serverHasNarrowedFlagSurface,
            "server build predates the narrowed AEL flag surface");

        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE).update(key)
                .bin(BIN_MAP).upsertFrom(ael)
                .execute());
        assertCompileDiagnostic(ex, ael, messageFragment);
    }

    private void assertSelectRejected(String ael, String messageFragment) {
        Assumptions.assumeTrue(serverHasNarrowedFlagSurface,
            "server build predates the narrowed AEL flag surface");

        AerospikeException ex = assertThrows(AerospikeException.class, () ->
            sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE).query(key)
                .bin("out").selectFrom(ael)
                .execute());
        assertCompileDiagnostic(ex, ael, messageFragment);
    }

    private static void assertCompileDiagnostic(AerospikeException ex, String ael, String messageFragment) {
        assertEquals(ResultCode.PARAMETER_ERROR, ex.getResultCode(),
            () -> "expected a compile rejection for AEL: " + ael);

        String msg = ex.getBaseMessage();
        assertNotNull(msg, () -> "expected a folded compile diagnostic for AEL: " + ael);
        assertTrue(msg.toLowerCase().contains(messageFragment.toLowerCase()),
            () -> "expected '" + messageFragment + "' in diagnostic for " + ael + ", got: " + msg);
    }

    private List<?> selectList(String ael) {
        Object value = selectValue(ael);
        assertInstanceOf(List.class, value, () -> "expected a list for AEL: " + ael);
        return (List<?>) value;
    }

    private Object selectValue(String ael) {
        Assumptions.assumeTrue(serverHasNarrowedFlagSurface,
            "server build predates the narrowed AEL flag surface");

        try (RecordStream rs = session.query(key)
            .bin("out")
            .selectFrom(ael)
            .execute()) {
            assertTrue(rs.hasNext(), () -> "no record for AEL: " + ael);
            Record rec = rs.next().recordOrThrow();
            Object value = rec.getValue("out");
            assertNotNull(value, () -> "null result for AEL: " + ael);
            return value;
        }
    }

    private static boolean probeSelectRejects(String ael, String messageFragment) {
        try {
            sessionWithVerbosity(ErrorDetailVerbosity.MESSAGE).query(key)
                .bin("_probe").selectFrom(ael)
                .execute();
            return false;
        }
        catch (AerospikeException ex) {
            if (ex.getResultCode() != ResultCode.PARAMETER_ERROR) {
                return false;
            }
            String msg = ex.getBaseMessage();
            return msg != null && msg.toLowerCase().contains(messageFragment.toLowerCase());
        }
    }

    private static Session sessionWithVerbosity(int verbosity) {
        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("errorDetail", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(verbosity)
            )
        );
        return cluster.createSession(behavior);
    }
}
