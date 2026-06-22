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
package com.aerospike.client.sdk.command;

import java.util.HashMap;
import java.util.Map;

/**
 * Parses {@code AS_MSG} field TLVs from a {@link RecordParser} buffer.
 * For internal use decoding query-plan probe replies and similar single-message responses.
 */
public final class MsgFieldParser {
    private final byte[] buffer;
    private final int offset;
    private final int fieldCount;
    private Map<Integer, byte[]> fields;

    public MsgFieldParser(byte[] buffer, int offset, int fieldCount) {
        this.buffer = buffer;
        this.offset = offset;
        this.fieldCount = fieldCount;
    }

    public static MsgFieldParser from(RecordParser parser) {
        return new MsgFieldParser(parser.dataBuffer, parser.dataOffset, parser.fieldCount);
    }

    /**
     * Returns the raw value bytes for a field type, or {@code null} if absent.
     * When multiple fields share the same type, the last one wins.
     */
    public byte[] getField(int fieldType) {
        ensureParsed();
        return fields.get(fieldType);
    }

    /**
     * Returns the UTF-8 string value for a field type, or {@code null} if absent.
     */
    public String getUtf8Field(int fieldType) {
        byte[] data = getField(fieldType);
        if (data == null) {
            return null;
        }
        return Buffer.utf8ToString(data, 0, data.length);
    }

    private void ensureParsed() {
        if (fields != null) {
            return;
        }

        fields = new HashMap<>(fieldCount);
        int pos = offset;

        for (int i = 0; i < fieldCount; i++) {
            int len = Buffer.bytesToInt(buffer, pos);
            pos += 4;
            int type = buffer[pos++] & 0xFF;
            int size = len - 1;

            if (size > 0) {
                byte[] value = new byte[size];
                System.arraycopy(buffer, pos, value, 0, size);
                fields.put(type, value);
            }
            else {
                fields.put(type, new byte[0]);
            }
            pos += size;
        }
    }
}
