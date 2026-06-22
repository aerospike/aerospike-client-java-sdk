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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class MsgFieldParserTest {

    @Test
    void parsesIndexNameAndRangeFields() {
        byte[] indexName = "age_idx".getBytes(StandardCharsets.UTF_8);
        byte[] range = new byte[] {1, 3, 'a', 'g', 'e', 3, 0, 0, 0, 8};

        byte[] buffer = buildMessage(indexName, FieldType.INDEX_NAME, range, FieldType.INDEX_RANGE);
        MsgFieldParser parser = new MsgFieldParser(buffer, 0, 2);

        assertEquals("age_idx", parser.getUtf8Field(FieldType.INDEX_NAME));
        assertArrayEquals(range, parser.getField(FieldType.INDEX_RANGE));
        assertNull(parser.getUtf8Field(FieldType.NAMESPACE));
    }

    @Test
    void emptyFieldCountReturnsNull() {
        MsgFieldParser parser = new MsgFieldParser(new byte[0], 0, 0);
        assertNull(parser.getField(FieldType.INDEX_NAME));
    }

    @Test
    void fromRecordParserUsesParserOffsets() {
        byte[] name = "idx".getBytes(StandardCharsets.UTF_8);
        byte[] buffer = buildMessage(name, FieldType.INDEX_NAME);

        int headerSize = Command.MSG_REMAINING_HEADER_SIZE;
        byte[] message = new byte[headerSize + buffer.length];
        System.arraycopy(buffer, 0, message, headerSize, buffer.length);

        RecordParser rp = new RecordParser(message, 0, message.length);
        MsgFieldParser parser = MsgFieldParser.from(rp);

        assertEquals("idx", parser.getUtf8Field(FieldType.INDEX_NAME));
    }

    private static byte[] buildMessage(byte[] value1, int type1) {
        return buildMessage(value1, type1, null, -1);
    }

    private static byte[] buildMessage(byte[] value1, int type1, byte[] value2, int type2) {
        int size1 = fieldSize(value1);
        int size2 = value2 == null ? 0 : fieldSize(value2);
        byte[] buffer = new byte[size1 + size2];
        int offset = 0;
        offset = writeField(buffer, offset, type1, value1);
        if (value2 != null) {
            writeField(buffer, offset, type2, value2);
        }
        return buffer;
    }

    private static int fieldSize(byte[] value) {
        return 4 + 1 + value.length;
    }

    private static int writeField(byte[] buffer, int offset, int type, byte[] value) {
        Buffer.intToBytes(value.length + 1, buffer, offset);
        offset += 4;
        buffer[offset++] = (byte) type;
        System.arraycopy(value, 0, buffer, offset, value.length);
        return offset + value.length;
    }
}
