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
package com.aerospike.client.sdk.mapper;

public class Address {
    private final String line1;
    private final String city;
    private final String state;
    private final String country;
    private final String zipCode;
    
    public Address(String line1, String city, String state, String country, String zipCode) {
        super();
        this.line1 = line1;
        this.city = city;
        this.state = state;
        this.country = country;
        this.zipCode = zipCode;
    }

    public String getLine1() {
        return line1;
    }
    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }
    public String getCountry() {
        return country;
    }
    public String getZipCode() {
        return zipCode;
    }
    @Override
    public String toString() {
        return "Address [line1=" + line1 + ", city=" + city + ", state=" + state + ", country=" + country + ", zipCode="
                + zipCode + "]";
    }
}
