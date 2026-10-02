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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.query.Filter;
import com.aerospike.client.sdk.util.Version;

public class BackgroundOperationBuilderValidationTest {
    private static final DataSet DATA_SET = DataSet.of("test", "bg_builder_validation");

    @Test
    void filterStartsUnset() {
        withSession(session ->
            assertNull(session.backgroundTask().update(DATA_SET).getFilter()));
    }

    @Test
    void filterReturnsSameBuilderAndStoresIdentity() {
        withSession(session -> {
            BackgroundOperationBuilder builder = session.backgroundTask().update(DATA_SET);
            Filter filter = Filter.equal("age", 30);

            assertSame(builder, builder.filter(filter));
            assertSame(filter, builder.getFilter());
        });
    }

    @Test
    void filterRejectsNull() {
        withSession(session ->
            assertThrows(NullPointerException.class,
                () -> session.backgroundTask().update(DATA_SET).filter(null)));
    }

    @Test
    void filterRejectsSecondCall() {
        withSession(session -> {
            BackgroundOperationBuilder builder = session.backgroundTask()
                .update(DATA_SET)
                .filter(Filter.equal("age", 30));

            assertThrows(IllegalArgumentException.class,
                () -> builder.filter(Filter.equal("age", 31)));
        });
    }

    @Test
    void filterCanBeDeclaredBeforeWhere() {
        withSession(session -> {
            Filter filter = Filter.equal("age", 30);

            BackgroundOperationBuilder builder = session.backgroundTask()
                .update(DATA_SET)
                .filter(filter)
                .where(Exp.eq(Exp.intBin("status"), Exp.val(1)));

            assertSame(filter, builder.getFilter());
        });
    }

    @Test
    void filterCanBeDeclaredAfterWhere() {
        withSession(session -> {
            Filter filter = Filter.equal("age", 30);

            BackgroundOperationBuilder builder = session.backgroundTask()
                .update(DATA_SET)
                .where(Exp.eq(Exp.intBin("status"), Exp.val(1)))
                .filter(filter);

            assertSame(filter, builder.getFilter());
        });
    }

    @Test
    void filterIsAvailableFromUpdateDeleteAndTouchEntryPoints() {
        withSession(session -> {
            Filter updateFilter = Filter.equal("age", 30);
            Filter deleteFilter = Filter.equal("age", 31);
            Filter touchFilter = Filter.equal("age", 32);

            assertSame(updateFilter, session.backgroundTask().update(DATA_SET).filter(updateFilter).getFilter());
            assertSame(deleteFilter, session.backgroundTask().delete(DATA_SET).filter(deleteFilter).getFilter());
            assertSame(touchFilter, session.backgroundTask().touch(DATA_SET).filter(touchFilter).getFilter());
        });
    }

    @Test
    void filterIsAvailableFromTypedDatasetEntryPoint() {
        withSession(session -> {
            TypedDataSet<Row> typedDataSet = TypedDataSet.of("test", "bg_builder_validation", Row.class);
            Filter filter = Filter.equal("age", 30);

            assertSame(filter, session.backgroundTask().update(typedDataSet).filter(filter).getFilter());
        });
    }

    private static void withSession(SessionConsumer consumer) {
        try (Cluster cluster = TestClusters.disconnected(Version.SERVER_VERSION_8_2)) {
            consumer.accept(cluster.createSession(Behavior.DEFAULT));
        }
    }

    private interface SessionConsumer {
        void accept(Session session);
    }

    private static final class Row {
    }
}
