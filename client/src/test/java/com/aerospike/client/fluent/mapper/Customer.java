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
package com.aerospike.client.fluent.mapper;

import java.util.Date;

public class Customer {
    private long id;
    private String name;
    private int age;
    private Date dob;
    private Address address;
    
    public Customer() {
        super();
    }
    public Customer(long id, String name, int age, Date dob) {
        this(id, name, age, dob, null);
    }
    public Customer(long id, String name, int age, Date dob, Address address) {
        super();
        this.id = id;
        this.name = name;
        this.age = age;
        this.dob = dob;
        this.address = address;
    }
    
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public Date getDob() {
        return dob;
    }
    public void setDob(Date dob) {
        this.dob = dob;
    }
    
    public Address getAddress() {
        return address;
    }
    public void setAddress(Address address) {
        this.address = address;
    }
    
    @Override
    public String toString() {
        return "Customer [id=" + id + ", name=" + name + ", age=" + age + ", dob=" + dob + ", address=" + address + "]";
    }
}