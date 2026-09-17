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

import org.junit.jupiter.api.Assumptions;

/**
 * Support for tests that assert behaviour the client currently gets wrong.
 *
 * <p>Some defects are worth pinning rather than leaving untested: the surrounding code paths still need
 * coverage, and a test that records what the client actually does today is the clearest description of the
 * bug. The risk is that such a test reads like an endorsement of the behaviour, and that whoever fixes the
 * defect gets an unexplained failure.</p>
 *
 * <p>So a pinned defect is marked two ways. {@link #TAG} makes the set findable
 * ({@code mvn test -Dgroups=known-defect}) and keeps it out of the body of tests that describe intended
 * behaviour. {@link #pinned} rewrites the failure so that whoever trips it is told what the test is for
 * rather than left with a bare comparison failure.</p>
 *
 * <p>There are three ways a defect gets marked here, depending on what the test can still do. Prefer the
 * earliest one that applies, because they lose progressively more coverage:</p>
 * <ul>
 *   <li><b>Pinned.</b> The test asserts the current, wrong behaviour through {@link #pinned} and passes.
 *       Preferred, because the surrounding code stays covered and the test fails the moment the defect is
 *       fixed.</li>
 *   <li><b>Skipped where it applies.</b> The defect only shows up in some environments, so {@link #skipWhere}
 *       skips the test there and lets it run and guard everywhere else. Use this rather than
 *       {@code @Disabled}, which is unconditional and would give up the environments that work.</li>
 *   <li><b>Disabled.</b> The intended behaviour cannot be reached at all, so the test would only fail. It
 *       keeps asserting what <em>should</em> happen and carries {@code @Disabled} with the defect in the
 *       reason. This buys no coverage, so use it only when neither of the above is possible.</li>
 * </ul>
 *
 * <p>Both carry {@code @Tag(KnownDefect.TAG)}. A test using this is <em>not</em> a substitute for filing
 * the defect.</p>
 */
public final class KnownDefect {

    /** Tag applied to every test that pins or is disabled by a defect. Nothing in the build filters on it. */
    public static final String TAG = "known-defect";

    private KnownDefect() {
    }

    /**
     * Runs assertions that describe incorrect behaviour, explaining the failure if they stop holding.
     *
     * @param defect     what the client does wrong, and what it should do instead
     * @param assertions assertions describing the current, incorrect behaviour
     */
    public static void pinned(String defect, Runnable assertions) {
        try {
            assertions.run();
        }
        catch (AssertionError e) {
            throw new AssertionError(
                "This test pins a known defect, so it asserts the current and incorrect behaviour.\n"
                    + "Defect: " + defect + "\n"
                    + "A failure here most likely means the defect has been fixed. If so, rewrite this test "
                    + "to assert the corrected behaviour and drop its @Tag(KnownDefect.TAG).\n"
                    + "Underlying failure: " + e.getMessage(),
                e);
        }
    }

    /**
     * Skips a test in the environments a defect affects, leaving it to run everywhere else.
     *
     * <p>For a test that is correct as written and fails only against, say, a strong-consistency namespace or
     * a particular server edition.</p>
     *
     * @param defectApplies whether the current environment is one the defect affects
     * @param defect        what goes wrong here, and where the fix belongs
     */
    public static void skipWhere(boolean defectApplies, String defect) {
        Assumptions.assumeFalse(defectApplies,
            "Skipped by a known defect that only affects this environment. The assertions below are correct "
                + "and still run elsewhere, so do not relax them to make this pass here.\nDefect: " + defect);
    }
}
