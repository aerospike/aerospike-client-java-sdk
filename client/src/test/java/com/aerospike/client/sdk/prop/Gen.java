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
package com.aerospike.client.sdk.prop;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * A value generator plus the shrink candidates for values it produced.
 *
 * <p>Deliberately dependency-free so property tests stay ordinary JUnit tests and keep
 * {@code ClusterTest}'s lifecycle and the {@code @RequiresServerFeature} gate. jqwik runs
 * as its own JUnit Platform engine and honours neither.
 *
 * @param <T> generated value type
 */
@FunctionalInterface
public interface Gen<T> {
    /** Produce one value. Must depend only on {@code rng} so a seed replays exactly. */
    T generate(Random rng);

    /**
     * Candidate simplifications of a failing value, roughly simplest first. Returning an
     * empty list disables shrinking. Candidates need not preserve failure — {@link Prop}
     * re-runs each one and keeps only those that still fail.
     */
    default List<T> shrink(T value) {
        return List.of();
    }

    /**
     * Strings chosen to break case mapping, normalization and codepoint-vs-byte indexing
     * rather than to look like realistic data. Uniform random text finds nothing here;
     * the interesting inputs are the ones where one codepoint is not one byte, not one
     * character after case mapping, or not one character after normalization.
     */
    static Gen<String> strings() {
        return new Gen<>() {
            @Override
            public String generate(Random rng) {
                int roll = rng.nextInt(100);

                if (roll < 18) {
                    return NASTY.get(rng.nextInt(NASTY.size()));
                }
                if (roll < 36) {
                    // Concatenations put a hostile codepoint next to ordinary text, which
                    // is where offset arithmetic tends to go wrong.
                    StringBuilder sb = new StringBuilder();
                    int parts = 2 + rng.nextInt(2);
                    for (int i = 0; i < parts; i++) {
                        sb.append(rng.nextBoolean()
                            ? NASTY.get(rng.nextInt(NASTY.size()))
                            : word(rng));
                    }
                    return sb.toString();
                }
                if (roll < 54) {
                    return word(rng);
                }
                if (roll < 66) {
                    return words(rng);
                }
                if (roll < 78) {
                    return digits(rng);
                }
                if (roll < 88) {
                    return whitespaceHeavy(rng);
                }
                return ascii(rng);
            }

            @Override
            public List<String> shrink(String value) {
                if (value.isEmpty()) {
                    return List.of();
                }

                // LinkedHashSet keeps simplest-first order while dropping duplicates,
                // which matter because several rules collapse to the same candidate.
                Set<String> out = new LinkedHashSet<>();
                out.add("");

                int half = value.length() / 2;
                if (half > 0) {
                    out.add(value.substring(0, half));
                    out.add(value.substring(half));
                }

                String asciiOnly = toAscii(value);
                if (!asciiOnly.equals(value)) {
                    out.add(asciiOnly);
                }

                String stripped = value.strip();
                if (!stripped.equals(value)) {
                    out.add(stripped);
                }

                // Single codepoint deletions, but only once the value is small enough
                // that the extra round trips are cheap.
                if (value.codePointCount(0, value.length()) <= 12) {
                    value.codePoints().forEach(cp -> {
                        String without = value.replace(new String(Character.toChars(cp)), "");
                        if (!without.equals(value)) {
                            out.add(without);
                        }
                    });
                }

                out.remove(value);
                return new ArrayList<>(out);
            }
        };
    }

    /**
     * Codepoint offsets for a string of {@code length}, covering both edges and just past
     * them. Fixed rather than random: the boundaries are the whole point, and leaving them
     * to chance means most runs miss them.
     */
    static List<Integer> offsetsFor(int length) {
        Set<Integer> out = new LinkedHashSet<>(List.of(0, length, length + 1, -1, -length - 1));
        if (length > 0) {
            out.add(length - 1);
            out.add(length / 2);
            out.add(-length);
        }
        return new ArrayList<>(out);
    }

    /** Codepoints where one character is not one byte, one case, or one normal form. */
    List<String> NASTY = List.of(
        "",
        " ",
        "  ",
        "\t",
        "\n",
        "a",
        "A",
        "ß",                    // uppercases to two characters
        "ﬁ",                    // ligature, uppercases to two characters
        "İ",                    // Turkish dotted capital I
        "ı",                    // Turkish dotless i
        "Å",                    // precomposed
        "A\u030A",              // decomposed: same NFC form as the line above
        "e\u0301",              // decomposed e-acute
        "é",                    // precomposed e-acute
        "😀",                   // outside the BMP: one codepoint, two Java chars
        "👨\u200D👩\u200D👧",   // ZWJ sequence
        "\u202Ertl\u202C",      // bidi override
        "日本語",
        "\u0000",               // NUL inside an otherwise valid string
        "a\u0000b",
        "İstanbul",
        "straße",
        "ﬁre"
    );

    private static String word(Random rng) {
        int len = 1 + rng.nextInt(8);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            char c = (char) ('a' + rng.nextInt(26));
            sb.append(rng.nextBoolean() ? Character.toUpperCase(c) : c);
        }
        return sb.toString();
    }

    private static String words(Random rng) {
        int count = 2 + rng.nextInt(3);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(rng.nextBoolean() ? " " : SEPARATORS.charAt(rng.nextInt(SEPARATORS.length())));
            }
            sb.append(word(rng));
        }
        return sb.toString();
    }

    private static String digits(Random rng) {
        StringBuilder sb = new StringBuilder();
        if (rng.nextInt(4) == 0) {
            sb.append('-');
        }
        int len = 1 + rng.nextInt(6);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('0' + rng.nextInt(10)));
        }
        if (rng.nextInt(4) == 0) {
            sb.append('.').append(rng.nextInt(1000));
        }
        return sb.toString();
    }

    private static String whitespaceHeavy(Random rng) {
        return " ".repeat(rng.nextInt(3)) + word(rng) + " ".repeat(1 + rng.nextInt(3));
    }

    private static String ascii(Random rng) {
        int len = rng.nextInt(12);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char) (0x20 + rng.nextInt(0x5F)));
        }
        return sb.toString();
    }

    private static String toAscii(String value) {
        StringBuilder sb = new StringBuilder();
        value.codePoints().forEach(cp -> sb.append(cp < 0x80 ? (char) cp : 'a'));
        return sb.toString();
    }

    String SEPARATORS = "-_.,;:/|";
}
