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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.CTX;
import com.aerospike.client.sdk.junit.RequiresServerFeature;
import com.aerospike.client.sdk.junit.ServerFeature;
import com.aerospike.client.sdk.operation.StringOperation;
import com.aerospike.client.sdk.operation.StringWriteFlags;
import com.aerospike.client.sdk.prop.Gen;
import com.aerospike.client.sdk.prop.Prop;

/**
 * Property: a string op means the same thing wherever the string lives.
 *
 * <p>The server reaches a top-level string bin and a string nested inside a list or map
 * through different code — the CTX forms dispatch via {@code as_bin_string_modify_ctx_tr}
 * and its read-side twin, which is why {@code OperateStringTest} covers them separately.
 * Two implementations of one contract is the shape worth testing differentially: the same
 * op on the same bytes must produce the same answer, including the same failure, whether
 * the operand is the bin itself, element 1 of a list, or the value at a map key.
 *
 * <p>This needs no expected values. Disagreement between the three placements is the bug
 * signal, so the check never has to encode what a correct result looks like — which is
 * what lets it run over generated input.
 *
 * <p>Errors are compared as well as values. An op that fails on a nested operand but
 * succeeds on a top-level one is exactly the kind of divergence being hunted, so a result
 * code is just another outcome to match.
 *
 * @see Prop for {@code -Dprop.tries} / {@code -Dprop.seed}
 */
@RequiresServerFeature(ServerFeature.STRING_OPS)
@DisplayName("string ops agree across top-level, list-nested and map-nested operands")
public class OperateStringCtxPropertyTest extends ClusterTest {

    private static final String TOP_BIN = "s";
    private static final String LIST_BIN = "l";
    private static final String MAP_BIN = "m";

    /** Fixed neighbours so a bad offset lands on recognisable data rather than nothing. */
    private static final String LEFT = "<<";
    private static final String RIGHT = ">>";
    private static final String MAP_KEY = "k";

    private static final int LIST_SLOT = 1;

    private Key key;

    /** Builds one op against a bin, with the CTX that reaches the nested placement. */
    private record StringOp(String name, BiFunction<String, CTX[], Operation> build) {
        Operation at(String bin, CTX... ctx) {
            return build.apply(bin, ctx);
        }
    }

    @Test
    @DisplayName("read ops")
    public void readOpsAgreeAcrossPlacements() {
        key = args.set.id("string-ctx-prop-read");

        Prop.forAll(Gen.strings())
            .named("string read ops across placements")
            .withTries(40)
            .check(value -> {
                seedAllPlacements(value);

                for (StringOp op : readOps(value)) {
                    Map<String, String> byPlacement = readOutcomes(op);
                    assertAgreement(op, value, byPlacement);
                }
            });
    }

    @Test
    @DisplayName("modify ops")
    public void modifyOpsAgreeAcrossPlacements() {
        key = args.set.id("string-ctx-prop-modify");

        Prop.forAll(Gen.strings())
            .named("string modify ops across placements")
            .withTries(20)
            .check(value -> {
                for (StringOp op : modifyOps(value)) {
                    Map<String, String> byPlacement = new LinkedHashMap<>();
                    byPlacement.put("top-level", modifyOutcome(value, op, TOP_BIN));
                    byPlacement.put("list[1]", modifyOutcome(value, op, LIST_BIN,
                        CTX.listIndex(LIST_SLOT)));
                    byPlacement.put("map[k]", modifyOutcome(value, op, MAP_BIN,
                        CTX.mapKey(Value.get(MAP_KEY))));

                    assertAgreement(op, value, byPlacement);
                }
            });
    }

    // --- op inventory ---

    /**
     * Offsets and needles are derived from the value so cases stay meaningful as it
     * changes: a needle taken from the string matches, and the offsets straddle both ends.
     */
    private static List<StringOp> readOps(String value) {
        int len = value.codePointCount(0, value.length());
        List<StringOp> ops = new ArrayList<>();

        ops.add(new StringOp("strlen", StringOperation::strlen));
        ops.add(new StringOp("byteLength", StringOperation::byteLength));
        ops.add(new StringOp("isNumeric", StringOperation::isNumeric));
        ops.add(new StringOp("isUpper", StringOperation::isUpper));
        ops.add(new StringOp("isLower", StringOperation::isLower));
        ops.add(new StringOp("toBlob", StringOperation::toBlob));
        ops.add(new StringOp("toInteger", StringOperation::toInteger));
        ops.add(new StringOp("toDouble", StringOperation::toDouble));
        ops.add(new StringOp("b64Decode", StringOperation::b64Decode));
        ops.add(new StringOp("split()", StringOperation::split));
        ops.add(new StringOp("split(' ')", (b, c) -> StringOperation.split(b, " ", c)));

        for (int offset : Gen.offsetsFor(len)) {
            ops.add(new StringOp("charAt(" + offset + ")",
                (b, c) -> StringOperation.charAt(b, offset, c)));
            ops.add(new StringOp("substr(" + offset + ")",
                (b, c) -> StringOperation.substr(b, offset, c)));
            ops.add(new StringOp("substr(0," + offset + ")",
                (b, c) -> StringOperation.substr(b, 0, offset, c)));
        }

        for (String needle : needles(value)) {
            ops.add(new StringOp("find(" + needle + ")",
                (b, c) -> StringOperation.find(b, needle, c)));
            ops.add(new StringOp("contains(" + needle + ")",
                (b, c) -> StringOperation.contains(b, needle, c)));
            ops.add(new StringOp("startsWith(" + needle + ")",
                (b, c) -> StringOperation.startsWith(b, needle, c)));
            ops.add(new StringOp("endsWith(" + needle + ")",
                (b, c) -> StringOperation.endsWith(b, needle, c)));
        }

        return ops;
    }

    private static List<StringOp> modifyOps(String value) {
        int len = value.codePointCount(0, value.length());
        int flags = StringWriteFlags.DEFAULT;
        List<StringOp> ops = new ArrayList<>();

        ops.add(new StringOp("upper", (b, c) -> StringOperation.upper(flags, b, c)));
        ops.add(new StringOp("lower", (b, c) -> StringOperation.lower(flags, b, c)));
        ops.add(new StringOp("caseFold", (b, c) -> StringOperation.caseFold(flags, b, c)));
        ops.add(new StringOp("normalizeNFC", (b, c) -> StringOperation.normalizeNFC(flags, b, c)));
        ops.add(new StringOp("trim", (b, c) -> StringOperation.trim(flags, b, c)));
        ops.add(new StringOp("trimStart", (b, c) -> StringOperation.trimStart(flags, b, c)));
        ops.add(new StringOp("trimEnd", (b, c) -> StringOperation.trimEnd(flags, b, c)));
        ops.add(new StringOp("append", (b, c) -> StringOperation.append(flags, b, "!", c)));
        ops.add(new StringOp("prepend", (b, c) -> StringOperation.prepend(flags, b, "!", c)));
        ops.add(new StringOp("padStart(8)",
            (b, c) -> StringOperation.padStart(flags, b, 8, "x", c)));
        ops.add(new StringOp("padEnd(8)",
            (b, c) -> StringOperation.padEnd(flags, b, 8, "x", c)));
        ops.add(new StringOp("repeat(2)", (b, c) -> StringOperation.repeat(flags, b, 2, c)));

        for (int offset : Gen.offsetsFor(len)) {
            ops.add(new StringOp("insert(" + offset + ")",
                (b, c) -> StringOperation.insert(flags, b, offset, "Z", c)));
            ops.add(new StringOp("overwrite(" + offset + ")",
                (b, c) -> StringOperation.overwrite(flags, b, offset, "Z", c)));
            ops.add(new StringOp("snip(0," + offset + ")",
                (b, c) -> StringOperation.snip(flags, b, 0, offset, c)));
        }

        for (String needle : needles(value)) {
            ops.add(new StringOp("replace(" + needle + ")",
                (b, c) -> StringOperation.replace(flags, b, needle, "Z", c)));
            ops.add(new StringOp("replaceAll(" + needle + ")",
                (b, c) -> StringOperation.replaceAll(flags, b, needle, "Z", c)));
        }

        return ops;
    }

    /** A needle drawn from the value (so it matches) plus one that cannot. */
    private static List<String> needles(String value) {
        List<String> out = new ArrayList<>();
        out.add("\u0001nope");

        if (!value.isEmpty()) {
            int firstCp = value.codePointAt(0);
            out.add(new String(Character.toChars(firstCp)));

            int cpCount = value.codePointCount(0, value.length());
            if (cpCount > 1) {
                int mid = value.offsetByCodePoints(0, cpCount / 2);
                out.add(value.substring(0, mid));
            }
        }
        return out;
    }

    // --- execution ---

    private void seedAllPlacements(String value) {
        List<Object> list = Arrays.asList(LEFT, value, RIGHT);
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(MAP_KEY, value);

        session.delete(key).execute();
        session.upsert(key)
            .bin(TOP_BIN).setTo(value)
            .bin(LIST_BIN).setTo(list)
            .bin(MAP_BIN).setTo(map)
            .execute();
    }

    /**
     * Fast path runs all three placements in one command; a single failure aborts the
     * whole command, so the slow path re-runs them separately to attribute the failure.
     */
    private Map<String, String> readOutcomes(StringOp op) {
        Map<String, String> out = new LinkedHashMap<>();

        try (RecordStream rs = session.query(key)
            .appendOperations(
                op.at(TOP_BIN),
                op.at(LIST_BIN, CTX.listIndex(LIST_SLOT)),
                op.at(MAP_BIN, CTX.mapKey(Value.get(MAP_KEY))))
            .execute()) {
            Record rec = rs.next().recordOrThrow();
            out.put("top-level", render(rec.operationResult(0).getValue()));
            out.put("list[1]", render(rec.operationResult(1).getValue()));
            out.put("map[k]", render(rec.operationResult(2).getValue()));
            return out;
        }
        catch (Exception batchFailure) {
            out.put("top-level", readOutcome(op, TOP_BIN));
            out.put("list[1]", readOutcome(op, LIST_BIN, CTX.listIndex(LIST_SLOT)));
            out.put("map[k]", readOutcome(op, MAP_BIN, CTX.mapKey(Value.get(MAP_KEY))));
            return out;
        }
    }

    private String readOutcome(StringOp op, String bin, CTX... ctx) {
        try (RecordStream rs = session.query(key).appendOperations(op.at(bin, ctx)).execute()) {
            return render(rs.next().recordOrThrow().operationResult(0).getValue());
        }
        catch (Exception ex) {
            return errorOf(ex);
        }
    }

    /** Re-seeds, applies the modify to one placement, and reads that placement back. */
    private String modifyOutcome(String value, StringOp op, String bin, CTX... ctx) {
        seedAllPlacements(value);

        try {
            session.upsert(key).appendOperations(op.at(bin, ctx)).execute();
        }
        catch (Exception ex) {
            return errorOf(ex);
        }

        try (RecordStream rs = session.query(key).execute()) {
            Record rec = rs.next().recordOrThrow();

            if (TOP_BIN.equals(bin)) {
                return render(rec.getValue(TOP_BIN));
            }
            if (LIST_BIN.equals(bin)) {
                return render(rec.getList(LIST_BIN).get(LIST_SLOT));
            }
            return render(rec.getMap(MAP_BIN).get(MAP_KEY));
        }
        catch (Exception ex) {
            return "READBACK:" + errorOf(ex);
        }
    }

    // --- reporting ---

    private static void assertAgreement(StringOp op, String value, Map<String, String> byPlacement) {
        List<String> distinct = byPlacement.values().stream().distinct().toList();
        if (distinct.size() == 1) {
            return;
        }

        StringBuilder detail = new StringBuilder()
            .append(op.name()).append(" disagrees across placements")
            .append("\n  value: ").append(render(value));
        byPlacement.forEach((placement, outcome) ->
            detail.append("\n  ").append(String.format("%-10s", placement)).append(" -> ").append(outcome));

        // Comparing against the top-level outcome makes the assertion message name a
        // reference behaviour rather than just reporting that three things differ.
        String reference = byPlacement.get("top-level");
        byPlacement.forEach((placement, outcome) ->
            assertEquals(reference, outcome, detail::toString));
    }

    private static String errorOf(Exception ex) {
        if (ex instanceof AerospikeException ae) {
            return "ERR:" + ae.getResultCode();
        }
        Throwable cause = ex.getCause();
        if (cause instanceof AerospikeException ae) {
            return "ERR:" + ae.getResultCode();
        }
        return "ERR:" + ex.getClass().getSimpleName();
    }

    private static String render(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[] bytes) {
            StringBuilder sb = new StringBuilder("blob[");
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.append(']').toString();
        }
        if (value instanceof String s) {
            StringBuilder sb = new StringBuilder("\"");
            s.codePoints().forEach(cp -> {
                if (cp >= 0x20 && cp < 0x7F) {
                    sb.appendCodePoint(cp);
                }
                else {
                    sb.append(String.format("\\u{%04X}", cp));
                }
            });
            return sb.append('"').toString();
        }
        return value.getClass().getSimpleName() + "(" + value + ")";
    }
}
