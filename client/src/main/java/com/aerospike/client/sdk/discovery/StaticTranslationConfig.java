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
package com.aerospike.client.sdk.discovery;

import java.util.List;

/**
 * Network translation constants.
 */
public final class StaticTranslationConfig {
    public static final String STATIC_MAP = "static-map";
    public static final String PORT_MAP = "port-map";
    public static final String PASS_THROUGH = "pass-through";
    public static final List<String> PROVIDERS = List.of(STATIC_MAP, PORT_MAP, PASS_THROUGH);
}
