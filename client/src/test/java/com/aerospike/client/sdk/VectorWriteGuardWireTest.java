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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.command.CommandBuffer;
import com.aerospike.client.sdk.command.PartitionFilter;
import com.aerospike.client.sdk.command.PartitionTracker;
import com.aerospike.client.sdk.command.QueryCommand;
import com.aerospike.client.sdk.exp.Exp;
import com.aerospike.client.sdk.exp.VectorExp;
import com.aerospike.client.sdk.policy.Behavior;
import com.aerospike.client.sdk.policy.ResolvedSettings;
import com.aerospike.client.sdk.query.QueryBuilder;
import com.aerospike.client.sdk.vector.Vector;
import com.aerospike.client.sdk.vector.VectorDistanceMetric;

/** Command-buffer VECTOR flag tests. */
class VectorWriteGuardWireTest {
    @Test
    void vectorDistanceQueryProjectionIsFlagged() {
        assertTrue(buildQueryBuffer(true).hasVector(),
            "a vector-distance projection must set the VECTOR flag");
    }

    @Test
    void plainQueryIsNotFlagged() {
        assertFalse(buildQueryBuffer(false).hasVector(),
            "a query with no vector payload must not set the VECTOR flag");
    }

    @Test
    void vectorFlagResetsWhenBufferIsReused() {
        Session session = new Session(null, Behavior.DEFAULT);
        DataSet dataSet = DataSet.of("test", "vec_guard");
        ResolvedSettings settings = Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);

        CommandBuffer buffer = new CommandBuffer();

        QueryBuilder vectorQuery = new QueryBuilder(session, dataSet);
        vectorQuery.bin("dist").selectFrom(VectorExp.distance(
            VectorDistanceMetric.EUCLIDEAN,
            Vector.ofFloat32(new float[] {0.0f, 0.0f}),
            Exp.vectorBin("embedding")));
        QueryCommand vectorCmd = new QueryCommand(null, dataSet, null, null, settings, vectorQuery);
        buffer.setQuery(vectorCmd, new PartitionTracker(vectorCmd, new Node[1], PartitionFilter.all()), null, 1L);
        assertTrue(buffer.hasVector());

        // Reuse clears the flag.
        QueryBuilder plainQuery = new QueryBuilder(session, dataSet);
        plainQuery.bin("copy").selectFrom(Exp.intBin("n"));
        QueryCommand plainCmd = new QueryCommand(null, dataSet, null, null, settings, plainQuery);
        buffer.setQuery(plainCmd, new PartitionTracker(plainCmd, new Node[1], PartitionFilter.all()), null, 2L);
        assertFalse(buffer.hasVector(), "hasVector must reset when the buffer builds a later non-vector command");
    }

    private static CommandBuffer buildQueryBuffer(boolean withVectorDistance) {
        Session session = new Session(null, Behavior.DEFAULT);
        DataSet dataSet = DataSet.of("test", "vec_guard");
        QueryBuilder builder = new QueryBuilder(session, dataSet);

        if (withVectorDistance) {
            builder.bin("dist").selectFrom(VectorExp.distance(
                VectorDistanceMetric.EUCLIDEAN,
                Vector.ofFloat32(new float[] {0.0f, 0.0f}),
                Exp.vectorBin("embedding")));
        }
        else {
            builder.bin("copy").selectFrom(Exp.intBin("n"));
        }

        ResolvedSettings settings = Behavior.DEFAULT.getSettings(
            Behavior.OpKind.READ, Behavior.OpShape.QUERY, Behavior.Mode.ANY);
        QueryCommand command = new QueryCommand(null, dataSet, null, null, settings, builder);

        PartitionTracker tracker = new PartitionTracker(command, new Node[1], PartitionFilter.all());
        CommandBuffer buffer = new CommandBuffer();
        buffer.setQuery(command, tracker, null, 1L);
        return buffer;
    }
}
