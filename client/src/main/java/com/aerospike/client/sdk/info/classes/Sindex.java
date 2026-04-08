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
package com.aerospike.client.sdk.info.classes;

import com.aerospike.client.sdk.info.annotations.Average;
import com.aerospike.client.sdk.info.annotations.FirstOf;
import com.aerospike.client.sdk.info.annotations.Key;
import com.aerospike.client.sdk.info.annotations.MustMatch;
import com.aerospike.client.sdk.info.annotations.Named;
import com.aerospike.client.sdk.query.IndexType;

public class Sindex {
    @Named("ns")
    @Key
    private String namespace;
    @Key
    @Named("indexname")
    private String indexName;
    @Key
    private String set;
    @Key
    private String bin;
    @MustMatch
    private IndexType type;
    @Named("indextype")
    @MustMatch
    private String indexType;
    private String context;
    private String exp;
    @FirstOf({"WO","RW"})
    private IndexState state;
    @Average
    private int entriesPerBval;
    public String getNamespace() {
        return namespace;
    }
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    public String getIndexName() {
        return indexName;
    }
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    public String getSet() {
        return set;
    }
    public void setSet(String set) {
        this.set = set;
    }
    public String getBin() {
        return bin;
    }
    public void setBin(String bin) {
        this.bin = bin;
    }
    public IndexType getType() {
        return type;
    }
    public void setType(IndexType type) {
        this.type = type;
    }
    public String getIndexType() {
        return indexType;
    }
    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }
    public String getContext() {
        return context;
    }
    public void setContext(String context) {
        this.context = context;
    }
    public String getExp() {
        return exp;
    }
    public void setExp(String exp) {
        this.exp = exp;
    }
    public IndexState getState() {
        return state;
    }
    public void setState(IndexState state) {
        this.state = state;
    }
    public int getEntriesPerBval() {
        return entriesPerBval;
    }
    public void setEntriesPerBval(int entriesPerBval) {
        this.entriesPerBval = entriesPerBval;
    }
    @Override
    public String toString() {
        return "Sindex [namespace=" + namespace + ", indexName=" + indexName + ", set=" + set + ", bin=" + bin
                + ", type=" + type + ", indexType=" + indexType + ", context=" + context + ", exp=" + exp + ", state="
                + state + ", entriesPerBval=" + entriesPerBval + "]";
    }
}