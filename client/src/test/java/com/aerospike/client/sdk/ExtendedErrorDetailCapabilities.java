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

import java.util.List;

import com.aerospike.client.sdk.exp.Expression;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.Behavior.Selectors;

/**
 * Runtime probes for server extended error-detail support (AER-6932 field 45).
 *
 * <p>Released 8.1.x images may not include batch extended errors yet; capability is
 * detected by issuing a batch read with an invalid filter expression at elevated
 * verbosity and inspecting the row error payload.</p>
 */
public final class ExtendedErrorDetailCapabilities {
    private static final String PROBE_KEY_PREFIX = "_eed_batch_probe_";
    private static final String PROBE_BIN = "eedv";

    /** Server-specific batch filter-build context (see {@code rw_utils.c}). */
    static final String BATCH_FILTER_BUILD_MESSAGE = "invalid filter expression in batch request";

    private static Boolean batchExtendedMessages;
    private static Boolean batchExpressionTrace;

    private ExtendedErrorDetailCapabilities() {
    }

    /**
     * Whether batch filter build failures return field-45 extended messages
     * (verbosity {@link ErrorDetailVerbosity#MESSAGE} or higher).
     */
    public static boolean supportsBatchExtendedErrorDetail(
        Cluster cluster, Session session, DataSet set
    ) {
        probeBatch(cluster, session, set);
        return batchExtendedMessages;
    }

    /**
     * Whether batch filter build failures return a structured expression trace
     * at {@link ErrorDetailVerbosity#EXPRESSION_TRACE}.
     */
    public static boolean supportsBatchExpressionTrace(
        Cluster cluster, Session session, DataSet set
    ) {
        probeBatch(cluster, session, set);
        return batchExpressionTrace;
    }

    private static void probeBatch(Cluster cluster, Session session, DataSet set) {
        if (batchExtendedMessages != null) {
            return;
        }

        batchExtendedMessages = false;
        batchExpressionTrace = false;

        String keyId1 = PROBE_KEY_PREFIX + "1";
        String keyId2 = PROBE_KEY_PREFIX + "2";
        List<Key> keys = set.ids(List.of(keyId1, keyId2));

        for (Key key : keys) {
            session.upsert(key)
                .bin(PROBE_BIN).setTo(1)
                .execute();
        }

        Behavior behavior = Behavior.DEFAULT.deriveWithChanges("eedBatchProbe", builder -> builder
            .on(Selectors.all(), ops -> ops
                .errorDetailVerbosity(ErrorDetailVerbosity.EXPRESSION_TRACE)
            )
        );

        Session verbose = cluster.createSession(behavior);
        Expression invalidExp = Expression.fromBytes(new byte[] {(byte)0xFF, (byte)0xFE, (byte)0xFD});

        try {
            // Multi-key batch path returns row errors in-stream; single-key query throws.
            RecordStream rs = verbose.query(keys)
                .where(invalidExp)
                .includeMissingKeys()
                .execute();

            if (!rs.hasNext()) {
                return;
            }

            RecordResult res = rs.next();

            if (res.getResultCode() != ResultCode.PARAMETER_ERROR) {
                return;
            }

            String msg = res.getMessage();

            if (msg != null && msg.contains(BATCH_FILTER_BUILD_MESSAGE)) {
                batchExtendedMessages = true;
            }

            batchExpressionTrace = res.getExpressionTrace() != null;
        }
        finally {
            session.delete(keys).execute();
        }
    }
}
