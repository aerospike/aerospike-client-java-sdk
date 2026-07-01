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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import com.aerospike.client.sdk.ClusterTest;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Session;

/**
 * Integration test: load a JSONL AEL corpus, submit each expression to the server
 * as a filter ({@code where}) and read ({@code selectFrom}) compile probe, and
 * assert {@code expect} labels ({@code parse-ok} / {@code parse-error}).
 *
 * <p>System properties:</p>
 * <ul>
 *   <li>{@code ael.corpus} — JSONL path (default: classpath {@code /ael-corpus/sample.jsonl})</li>
 *   <li>{@code ael.corpus.id} — run only this entry id (for corpus-walk loops)</li>
 *   <li>{@code ael.corpus.skipPrefix} — skip ids with this prefix (e.g. {@code seed-})</li>
 *   <li>{@code ael.corpus.strictKinds=true} — assert {@code expect_kind} on parse-error rows</li>
 * </ul>
 */
public class AelCorpusVerifyTest extends ClusterTest {
    private static final String DEFAULT_CORPUS_RESOURCE = "/ael-corpus/sample.jsonl";
    private static final String CORPUS_PROPERTY = "ael.corpus";
    private static final String CORPUS_ID_PROPERTY = "ael.corpus.id";
    private static final String CORPUS_SKIP_PREFIX_PROPERTY = "ael.corpus.skipPrefix";
    private static final String KEY_NAME = "ael_corpus_verify";
    private static final String BIN_MARKER = "marker";

    private static List<AelCorpusEntry> corpus;

    private Key key;

    @BeforeAll
    public static void requireAelServerAndLoadCorpus() throws IOException {
        Assumptions.assumeTrue(cluster.supportsAel(), "cluster does not report AEL support");
        corpus = loadCorpus();
        assertTrue(!corpus.isEmpty(), "corpus is empty");
    }

    @BeforeEach
    public void seedRecord() {
        key = args.set.id(KEY_NAME);
        session.delete(key).execute();
        sessionWithSendKey.delete(key).execute();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("alpha", 10);
        map.put("beta", 20);
        map.put("gamma", 30);
        map.put("meta", 5);

        var upsert = session.upsert(key)
            .bin(BIN_MARKER).setTo(1)
            .bin("l").setTo(List.of(100, 200, 300, 400, 500))
            .bin("m").setTo(map)
            .bin("items").setTo(List.of(10, 20, 30));
        sessionWithSendKey.upsert(key)
            .bin(BIN_MARKER).setTo(1)
            .bin("l").setTo(List.of(100, 200, 300, 400, 500))
            .bin("m").setTo(map)
            .bin("items").setTo(List.of(10, 20, 30))
            .execute();
        upsert.execute();
    }

    @TestFactory
    Stream<DynamicTest> verifyCorpusEntriesOneByOne() {
        List<DynamicTest> tests = new ArrayList<>();
        for (AelCorpusEntry entry : corpus) {
            tests.add(dynamicTest(entry.id() + " [" + entry.expect() + "]", () -> {
                String failure = verifyOne(entry);
                assertNull(failure, failure);
            }));
        }
        return tests.stream();
    }

    String verifyOne(AelCorpusEntry entry) {
        Session probeSession = sessionFor(entry);
        AelCorpusVerifier.ProbeResult result =
            AelCorpusVerifier.probeCompile(probeSession, key, entry.expr());
        return AelCorpusVerifier.verifyEntry(entry, result);
    }

    private static Session sessionFor(AelCorpusEntry entry) {
        String expr = entry.expr();
        if (expr.contains("$.key()") || expr.contains("$.keyExists()")) {
            return sessionWithSendKey;
        }
        return session;
    }

    private static List<AelCorpusEntry> loadCorpus() throws IOException {
        List<AelCorpusEntry> loaded;
        String override = System.getProperty(CORPUS_PROPERTY);
        if (override != null && !override.isBlank()) {
            loaded = AelCorpusEntry.readJsonl(Path.of(override));
        }
        else {
            InputStream in = AelCorpusVerifyTest.class.getResourceAsStream(DEFAULT_CORPUS_RESOURCE);
            if (in == null) {
                throw new IOException("missing classpath resource " + DEFAULT_CORPUS_RESOURCE);
            }
            try (InputStream stream = in) {
                loaded = AelCorpusEntry.readJsonl(stream);
            }
        }

        String onlyId = System.getProperty(CORPUS_ID_PROPERTY);
        if (onlyId != null && !onlyId.isBlank()) {
            loaded = loaded.stream().filter(e -> onlyId.equals(e.id())).toList();
            if (loaded.isEmpty()) {
                throw new IOException("no corpus entry with id " + onlyId);
            }
        }

        String skipPrefix = System.getProperty(CORPUS_SKIP_PREFIX_PROPERTY);
        if (skipPrefix != null && !skipPrefix.isBlank()) {
            loaded = loaded.stream().filter(e -> !e.id().startsWith(skipPrefix)).toList();
        }

        return loaded;
    }
}
