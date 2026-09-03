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
package com.aerospike.client.sdk.command;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.Key;
import com.aerospike.client.sdk.Record;
import com.aerospike.client.sdk.ResultCode;
import com.aerospike.client.sdk.exp.Expression;

public class CommandContainerToStringTest {
    @Test
    public void txnIncludesStateAndTrackedKeyCounts() {
        Txn txn = new Txn();
        txn.setNamespace("test");
        txn.setTimeout(12);
        txn.setDeadline(34);
        txn.setInDoubt(true);
        txn.onRead(new Key("test", "users", "read-key"), 1L);
        txn.onWriteInDoubt(new Key("test", "users", "write-key"));

        assertThat(txn.toString())
            .contains("Txn{id=")
            .contains("state=OPEN")
            .contains("namespace=test")
            .contains("timeout=12")
            .contains("deadline=34")
            .contains("writeInDoubt=true")
            .contains("inDoubt=true")
            .contains("reads=1")
            .contains("writes=1");
    }

    @Test
    public void infoErrorIncludesParsedCodeAndMessage() {
        Info.Error error = new Info.Error("ERROR:123:bad things happened");

        assertThat(error.toString())
            .isEqualTo("Info.Error{code=123, message=bad things happened}");
    }

    @Test
    public void batchRecordIncludesStatusAndOptionalPayloads() {
        Key key = new Key("test", "users", "batch-key");
        Expression where = Expression.fromBytes(new byte[] {1, 2, 3});
        BatchRecord record = new BatchRecord(key, where, new BatchAttr());
        record.setRecord(new Record(2, 3));

        assertThat(record.toString())
            .contains("BatchRecord{type=null")
            .contains("key=" + key)
            .contains("resultCode=" + ResultCode.OK)
            .contains("subCode=0")
            .contains("inDoubt=false")
            .contains("hasWrite=false")
            .contains("linearize=false")
            .contains("record=(gen:2),(exp:3)")
            .contains("where=" + where.getBase64());
    }

    @Test
    public void batchResultsIncludesStatusCountAndRecords() {
        Key key = new Key("test", "users", "batch-key");
        BatchRecord record = new BatchRecord(key, false);

        assertThat(new BatchResults(new BatchRecord[] {record}, false).toString())
            .contains("BatchResults{status=false")
            .contains("recordCount=1")
            .contains("records=[BatchRecord{type=null, key=" + key);
    }
}
