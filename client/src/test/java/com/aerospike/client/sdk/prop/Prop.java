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
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.opentest4j.AssertionFailedError;

/**
 * Minimal property runner: generate values, run a check, and on failure shrink to the
 * simplest value that still fails before reporting.
 *
 * <p>Runs inside an ordinary JUnit test method, so server-backed properties keep
 * {@code ClusterTest}'s cluster lifecycle and the {@code @RequiresServerFeature} gate.
 *
 * <p>Case count and seed are overridable:
 * <pre>
 *   -Dprop.tries=500
 *   -Dprop.seed=1234567
 * </pre>
 * The seed is printed with every failure so a CI run can be replayed exactly.
 *
 * @param <T> generated value type
 */
public final class Prop<T> {
    private static final int DEFAULT_TRIES = 60;
    private static final int MAX_SHRINK_STEPS = 60;

    private final Gen<T> gen;
    private String name = "property";
    private int tries = Integer.getInteger("prop.tries", DEFAULT_TRIES);
    private long seed = Long.getLong("prop.seed", System.nanoTime());

    private Prop(Gen<T> gen) {
        this.gen = gen;
    }

    public static <T> Prop<T> forAll(Gen<T> gen) {
        return new Prop<>(gen);
    }

    public Prop<T> named(String name) {
        this.name = name;
        return this;
    }

    /** Overrides the default case count; a {@code -Dprop.tries} value still wins. */
    public Prop<T> withTries(int tries) {
        this.tries = Integer.getInteger("prop.tries", tries);
        return this;
    }

    public Prop<T> withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * Runs {@code body} over generated values. On the first failure, shrinks and rethrows
     * an error describing the simplest failing value rather than the generated one.
     */
    public void check(Consumer<T> body) {
        Random rng = new Random(seed);

        for (int i = 0; i < tries; i++) {
            T value = gen.generate(rng);
            Throwable failure = runCatching(body, value);

            if (failure != null) {
                T simplest = shrink(body, value);
                throw report(simplest, value, i, runCatching(body, simplest), failure);
            }
        }
    }

    /**
     * Greedy descent: repeatedly replace the failing value with the first candidate that
     * also fails. Candidates are ordered simplest first, so this settles on a small value
     * without searching the whole space.
     */
    private T shrink(Consumer<T> body, T failing) {
        T current = failing;

        for (int step = 0; step < MAX_SHRINK_STEPS; step++) {
            T next = null;

            for (T candidate : gen.shrink(current)) {
                if (runCatching(body, candidate) != null) {
                    next = candidate;
                    break;
                }
            }

            if (next == null) {
                return current;
            }
            current = next;
        }

        return current;
    }

    private Throwable runCatching(Consumer<T> body, T value) {
        try {
            body.accept(value);
            return null;
        }
        catch (Throwable t) {
            return t;
        }
    }

    private AssertionFailedError report(T simplest, T original, int caseIndex,
            Throwable shrunkFailure, Throwable originalFailure) {
        Throwable cause = shrunkFailure != null ? shrunkFailure : originalFailure;

        String message = String.format(
            "%s failed on case %d of %d%n"
                + "  shrunk value : %s%n"
                + "  original     : %s%n"
                + "  replay with  : -Dprop.seed=%d -Dprop.tries=%d%n"
                + "  cause        : %s",
            name, caseIndex + 1, tries,
            render(simplest), render(original), seed, tries,
            cause.getMessage());

        AssertionFailedError error = new AssertionFailedError(message);
        error.initCause(cause);
        return error;
    }

    /** Renders control characters and non-ASCII escaped so failures are copy-pasteable. */
    private static String render(Object value) {
        if (!(value instanceof String s)) {
            return String.valueOf(value);
        }

        StringBuilder sb = new StringBuilder("\"");
        s.codePoints().forEach(cp -> {
            if (cp == '"') {
                sb.append("\\\"");
            }
            else if (cp == '\\') {
                sb.append("\\\\");
            }
            else if (cp >= 0x20 && cp < 0x7F) {
                sb.appendCodePoint(cp);
            }
            else {
                sb.append(String.format("\\u{%04X}", cp));
            }
        });
        return sb.append("\" (").append(s.codePointCount(0, s.length())).append(" cp)").toString();
    }

    /** Convenience for properties that enumerate a fixed list rather than generating. */
    public static <T> List<T> concat(List<T> a, List<T> b) {
        List<T> out = new ArrayList<>(a);
        out.addAll(b);
        return out;
    }
}
