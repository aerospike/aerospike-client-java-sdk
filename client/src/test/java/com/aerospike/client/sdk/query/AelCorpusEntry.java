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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.yaml.snakeyaml.Yaml;

/**
 * One labeled row from an AEL corpus JSONL file (see dsl-test/ael-corpus-gen).
 */
public record AelCorpusEntry(
    String id,
    String mode,
    String expect,
    String expr,
    String expectKind
) {
    private static final Yaml YAML = new Yaml();

    public static List<AelCorpusEntry> readJsonl(InputStream in) throws IOException {
        Objects.requireNonNull(in, "in");
        List<AelCorpusEntry> entries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            int lineNo = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                line = line.strip();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                entries.add(parseLine(line, lineNo));
            }
        }
        return entries;
    }

    public static List<AelCorpusEntry> readJsonl(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        try (InputStream in = Files.newInputStream(path)) {
            return readJsonl(in);
        }
    }

    private static AelCorpusEntry parseLine(String line, int lineNo) {
        Object loaded = YAML.load(line);
        if (!(loaded instanceof Map<?, ?> row)) {
            throw new IllegalArgumentException("line " + lineNo + ": expected JSON object");
        }

        String id = requiredString(row, "id", lineNo);
        String mode = requiredString(row, "mode", lineNo);
        String expect = requiredString(row, "expect", lineNo);
        String expr = requiredString(row, "expr", lineNo);
        String expectKind = optionalString(row, "expect_kind");

        if (!"parse-ok".equals(expect) && !"parse-error".equals(expect)) {
            throw new IllegalArgumentException(
                "line " + lineNo + ": unsupported expect label '" + expect + "'");
        }

        return new AelCorpusEntry(id, mode, expect, expr, expectKind);
    }

    private static String requiredString(Map<?, ?> row, String key, int lineNo) {
        Object value = row.get(key);
        if (!(value instanceof String s) || s.isEmpty()) {
            throw new IllegalArgumentException("line " + lineNo + ": missing or empty '" + key + "'");
        }
        return s;
    }

    private static String optionalString(Map<?, ?> row, String key) {
        Object value = row.get(key);
        return value instanceof String s ? s : null;
    }
}
