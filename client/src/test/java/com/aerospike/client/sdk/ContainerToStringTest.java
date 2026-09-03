/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.cdt.ListOrder;
import com.aerospike.client.sdk.cdt.MapOrder;

public class ContainerToStringTest {
    private static final class Customer {
    }

    @Test
    public void typedKeyIncludesKeyAndEntityClass() {
        Key key = new Key("test", "users", "u1");
        TypedKey<Customer> typedKey = new TypedKey<>(key, Customer.class);

        assertThat(typedKey.toString())
            .contains("TypedKey{key=" + key)
            .contains("entityClass=" + Customer.class.getName());
    }

    @Test
    public void operationResultFormatsBytes() {
        assertThat(new OperationResult(new byte[] {1, 2, 3}).toString())
            .isEqualTo("OperationResult{value=bytes[len=3, hex=010203]}");
    }

    @Test
    public void recordFormatsByteArrayBins() {
        Record record = new Record(
            Map.of("blob", new byte[] {1, 2, 3}),
            new OperationResult[0],
            4,
            5);

        assertThat(record.toString())
            .contains("(gen:4)")
            .contains("(exp:5)")
            .contains("(blob:bytes[len=3, hex=010203])")
            .doesNotContain("[B@");
    }

    @Test
    public void recordResultIncludesStatusAndPayloadSummary() {
        Key key = new Key("test", "users", "u2");
        Record record = new Record(Map.of("name", "Ana"), new OperationResult[0], 6, 7);
        RecordResult result = new RecordResult(key, record, 9);

        assertThat(result.toString())
            .contains("RecordResult{key=" + key)
            .contains("resultCode=" + ResultCode.OK)
            .contains("subCode=" + SubCode.NONE)
            .contains("index=9")
            .contains("inDoubt=false")
            .contains("record=(gen:6),(exp:7)");
    }

    @Test
    public void objectWithMetadataIncludesObjectAndRecordMetadata() {
        Record record = new Record(10, 11);
        RecordStream.ObjectWithMetadata<String> withMetadata =
            new RecordStream.ObjectWithMetadata<>("customer", record);

        assertThat(withMetadata.toString())
            .isEqualTo("ObjectWithMetadata{object=customer, generation=10, expiration=11}");
    }

    @Test
    public void aerospikeListIncludesMetadataAndFormatsElements() {
        AerospikeList<Object> list = new AerospikeList<>(2, ListOrder.ORDERED);
        list.setPersistIndex(true);
        list.add("a");
        list.add(new byte[] {10, 11});

        assertThat(list.toString())
            .isEqualTo("AerospikeList{order=ORDERED, persistIndex=true, values=[a, bytes[len=2, hex=0a0b]]}");
    }

    @Test
    public void aerospikeMapIncludesMetadataAndFormatsEntries() {
        AerospikeMap<String, Object> map = AerospikeMap.of(MapOrder.KEY_VALUE_ORDERED, 1);
        map.persistIndex();
        map.put("blob", new byte[] {10, 11});

        assertThat(map.toString())
            .isEqualTo("AerospikeMap{order=KEY_VALUE_ORDERED, persistIndex=true, values={blob=bytes[len=2, hex=0a0b]}}");
    }

    @Test
    public void binToStringCapsLongValues() {
        String s = new Bin("name", "x".repeat(300)).toString();

        assertThat(s)
            .startsWith("name:" + "x".repeat(256))
            .endsWith("...");
        assertThat(s.length()).isLessThan(300);
    }

    @Test
    public void operationIncludesTypeBinAndValue() {
        Operation op = Operation.put(new Bin("blob", new byte[] {1, 2, 3}));

        assertThat(op.toString())
            .isEqualTo("Operation{type=WRITE, binName=blob, value=010203}");
    }

    @Test
    public void recordReadContextIncludesSessionIdentityAndEntityClass() {
        Session session = new Session(null, null);
        RecordReadContext<Customer> ctx = new RecordReadContext<>(session, Customer.class);

        assertThat(ctx.toString())
            .isEqualTo("RecordReadContext{session=" + Session.class.getName() +
                '@' + Integer.toHexString(System.identityHashCode(session)) +
                ", entityClass=" + Customer.class.getName() +
                '}');
    }
}
