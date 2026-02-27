package com.aerospike.examples.ecommerce;

public class Customer {
    private String id;
    private String name;
    private String email;
    private long creditLimitCents;
    private long balanceCents;

    public Customer() {}

    public Customer(String id, String name, String email, long creditLimitCents, long balanceCents) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.creditLimitCents = creditLimitCents;
        this.balanceCents = balanceCents;
    }

    public String getId()               { return id; }
    public String getName()             { return name; }
    public String getEmail()            { return email; }
    public long getCreditLimitCents()   { return creditLimitCents; }
    public long getBalanceCents()       { return balanceCents; }

    public void setId(String id)                        { this.id = id; }
    public void setName(String name)                    { this.name = name; }
    public void setEmail(String email)                  { this.email = email; }
    public void setCreditLimitCents(long limit)         { this.creditLimitCents = limit; }
    public void setBalanceCents(long balance)            { this.balanceCents = balance; }

    @Override
    public String toString() {
        return String.format("Customer[%s, %s, balance=$%.2f, limit=$%.2f]",
            id, name, balanceCents / 100.0, creditLimitCents / 100.0);
    }
}
