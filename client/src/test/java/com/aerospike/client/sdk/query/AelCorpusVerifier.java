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

import com.aerospike.client.sdk.AerospikeException;
import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.RecordStream;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.Session;

/**
 * Submits corpus expressions to the server and classifies compile-time acceptance
 * vs rejection. Probes both filter ({@code where}) and read ({@code selectFrom})
 * paths because corpus entries may be valid in only one context.
 *
 * <p>Only {@link ResultCode#PARAMETER_ERROR} is treated as a compile/parse rejection.
 * Other result codes ({@link ResultCode#OP_NOT_APPLICABLE},
 * {@link ResultCode#FILTERED_OUT}, etc.) mean the server accepted the expression and
 * rejected it at evaluation time — that still counts as {@code parse-ok}.</p>
 */
public final class AelCorpusVerifier {
    public static final String STRICT_KINDS_PROPERTY = "ael.corpus.strictKinds";

    public enum Outcome {
        /** Server accepted the expression (evaluation may still fail). */
        ACCEPTED,
        /** Server rejected at compile/parse time. */
        PARSE_ERROR
    }

    public record ProbeResult(Result filter, Result read) {
        public boolean accepted() {
            return filter.accepted() || read.accepted();
        }

        public String detail() {
            if (accepted()) {
                if (filter.accepted()) {
                    return "filter: " + filter.detail();
                }
                return "read: " + read.detail();
            }
            return "filter: " + filter.detail() + "; read: " + read.detail();
        }
    }

    public record Result(Outcome outcome, AerospikeException error) {
        public boolean accepted() {
            return outcome == Outcome.ACCEPTED;
        }

        public String detail() {
            if (error == null) {
                return Outcome.ACCEPTED.name();
            }
            String message = error.getMessage();
            if (message == null || message.isBlank()) {
                return ResultCode.getResultString(error.getResultCode());
            }
            return ResultCode.getResultString(error.getResultCode()) + ": " + message;
        }
    }

    private AelCorpusVerifier() {
    }

    public static ProbeResult probeCompile(Session session, Key key, String expr) {
        return new ProbeResult(
            probeFilter(session, key, expr),
            probeRead(session, key, expr));
    }

    public static Result probeFilter(Session session, Key key, String expr) {
        try {
            RecordStream rs = session.query(key).where(expr).execute();
            try {
                while (rs.hasNext()) {
                    rs.next();
                }
            }
            finally {
                rs.close();
            }
            return accepted(null);
        }
        catch (AerospikeException ae) {
            return classify(ae);
        }
    }

    public static Result probeRead(Session session, Key key, String expr) {
        try {
            RecordStream rs = session.query(key)
                .bin("_ael_probe")
                .selectFrom(expr)
                .execute();
            try {
                while (rs.hasNext()) {
                    rs.next();
                }
            }
            finally {
                rs.close();
            }
            return accepted(null);
        }
        catch (AerospikeException ae) {
            return classify(ae);
        }
    }

    private static Result classify(AerospikeException ae) {
        if (ae.getResultCode() == ResultCode.PARAMETER_ERROR) {
            return new Result(Outcome.PARSE_ERROR, ae);
        }
        return accepted(ae);
    }

    private static Result accepted(AerospikeException ae) {
        return new Result(Outcome.ACCEPTED, ae);
    }

    public static String verifyEntry(AelCorpusEntry entry, ProbeResult result) {
        return switch (entry.expect()) {
            case "parse-ok" -> verifyParseOk(entry, result);
            case "parse-error" -> verifyParseError(entry, result);
            default -> entry.id() + ": unsupported expect label '" + entry.expect() + "'";
        };
    }

    private static String verifyParseOk(AelCorpusEntry entry, ProbeResult result) {
        if (result.accepted()) {
            return null;
        }
        return entry.id() + ": expected parse-ok but got " + result.detail()
            + "\n  expr: " + entry.expr();
    }

    private static String verifyParseError(AelCorpusEntry entry, ProbeResult result) {
        if (result.accepted()) {
            return entry.id() + ": expected parse-error but server accepted (" + result.detail() + ")"
                + "\n  expr: " + entry.expr();
        }
        if (entry.expectKind() != null && Boolean.getBoolean(STRICT_KINDS_PROPERTY)) {
            AerospikeException error = pickParseError(result);
            if (error != null) {
                String message = error.getMessage();
                if (message != null && !messageContainsKind(message, entry.expectKind())) {
                    return entry.id() + ": parse-error ok but expect_kind '" + entry.expectKind()
                        + "' not found in message: " + message
                        + "\n  expr: " + entry.expr();
                }
            }
        }
        return null;
    }

    private static AerospikeException pickParseError(ProbeResult result) {
        if (result.filter().outcome() == Outcome.PARSE_ERROR) {
            return result.filter().error();
        }
        if (result.read().outcome() == Outcome.PARSE_ERROR) {
            return result.read().error();
        }
        return null;
    }

    private static boolean messageContainsKind(String message, String expectKind) {
        String lower = message.toLowerCase();
        if (lower.contains(expectKind.toLowerCase())) {
            return true;
        }
        for (String token : expectKind.split("-")) {
            if (!token.isEmpty() && lower.contains(token.toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}
