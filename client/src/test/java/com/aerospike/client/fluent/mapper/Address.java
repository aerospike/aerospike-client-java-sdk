package com.aerospike.client.fluent.mapper;

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
