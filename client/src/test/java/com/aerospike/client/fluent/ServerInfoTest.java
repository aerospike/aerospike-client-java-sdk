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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.aerospike.client.fluent.command.Info;
import com.aerospike.client.fluent.util.Version;

public class ServerInfoTest extends ClusterTest {
    @Test
    public void serverInfo() {
        Node node = cluster.getNodes()[0];
        getNamespaceConfig(node);
    }

    private void getNamespaceConfig(Node node) {
        String filter = "namespace/" + args.namespace;
        String tokens = Info.request(node, filter);
        assertNotNull(tokens);
        logNameValueTokens(tokens);
    }

    private void logNameValueTokens(String tokens) {
        String[] values = tokens.split(";");

        for (String value : values) {
            assertNotNull(value);
        }
    }

    @Test
    public void errorResponse() {
        Info.Error error;

        error = new Info.Error("FaIL:201:index not found");
        assertEquals(201, error.code);
        assertEquals("index not found", error.message);

        error = new Info.Error("ERRor:201:index not found");
        assertEquals(201, error.code);
        assertEquals("index not found", error.message);

        error = new Info.Error("error::index not found ");
        assertEquals(ResultCode.CLIENT_ERROR, error.code);
        assertEquals("index not found", error.message);

        error = new Info.Error("error: index not found ");
        assertEquals(ResultCode.CLIENT_ERROR, error.code);
        assertEquals("index not found", error.message);

        error = new Info.Error("error:99");
        assertEquals(99, error.code);
        assertEquals("error:99", error.message);

        error = new Info.Error("generic message");
        assertEquals(ResultCode.CLIENT_ERROR, error.code);
        assertEquals("generic message", error.message);
    }

    @Test
    public void validateServerBuilds() {
        List<String> builds = Arrays.asList("7.0.0.26", "8.1.0.0", "8.1.0.0-rc2");
        for (String build : builds) {
            Version ver = new Version(build);
            assertNotNull(ver);
            assertTrue(build.startsWith(ver.toString()));
        }
    }

    @Test
    public void invalidateServerBuilds() {
        List<String> builds = Arrays.asList("7.0.26", "8.1.C.0", "lol");
        for (String build : builds) {
            Version ver = new Version(build);
            assertNotNull(ver);
            assertFalse(build.startsWith(ver.toString()));
        }
    }
}