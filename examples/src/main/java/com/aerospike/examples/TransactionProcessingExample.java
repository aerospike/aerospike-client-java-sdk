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
package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordMapper;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.TypeSafeDataSet;
import com.aerospike.client.fluent.Value;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.util.MapUtil;

public class TransactionProcessingExample {
    public static class Transaction {
        private String id;
        private String desc;
        private long amountInCents;
        private long date;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getDesc() { return desc; }
        public void setDesc(String desc) { this.desc = desc; }
        public long getAmountInCents() { return amountInCents; }
        public void setAmountInCents(long amountInCents) { this.amountInCents = amountInCents; }
        public long getDate() { return date; }
        public void setDate(long date) { this.date = date; }
    }

    public static class TransactionMapper implements RecordMapper<Transaction> {
        @Override
        public Transaction fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Transaction txn = new Transaction();
            txn.setId(MapUtil.asString(map, "id"));
            txn.setDesc(MapUtil.asString(map, "desc"));
            txn.setAmountInCents(MapUtil.asLong(map, "amountInCents"));
            txn.setDate(MapUtil.asLong(map, "date"));
            return txn;
        }

        @Override
        public Map<String, Value> toMap(Transaction txn) {
            return MapUtil.buildMap()
                .add("id", txn.getId())
                .add("desc", txn.getDesc())
                .add("amountInCents", txn.getAmountInCents())
                .add("date", txn.getDate())
                .done();
        }

        @Override
        public Object id(Transaction txn) {
            return txn.getId();
        }
    }
    
    public static class Customer {
        private String customerId;
        private String firstName;
        private String lastName;
        private String email;
        private String phone;
        private long totalSpendInCents;
        private String statusLevel;

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getFirstName() { return firstName; }
        public void setFirstName(String firstName) { this.firstName = firstName; }
        public String getLastName() { return lastName; }
        public void setLastName(String lastName) { this.lastName = lastName; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
        public long getTotalSpendInCents() { return totalSpendInCents; }
        public void setTotalSpendInCents(long totalSpendInCents) { this.totalSpendInCents = totalSpendInCents; }
        public String getStatusLevel() { return statusLevel; }
        public void setStatusLevel(String statusLevel) { this.statusLevel = statusLevel; }
    }

    public static class CustomerMapper implements RecordMapper<Customer> {
        @Override
        public Customer fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Customer cust = new Customer();
            cust.setCustomerId(MapUtil.asString(map, "customerId"));
            cust.setFirstName(MapUtil.asString(map, "firstName"));
            cust.setLastName(MapUtil.asString(map, "lastName"));
            cust.setEmail(MapUtil.asString(map, "email"));
            cust.setPhone(MapUtil.asString(map, "phone"));
            cust.setTotalSpendInCents(MapUtil.asLong(map, "totalSpend"));
            cust.setStatusLevel(MapUtil.asString(map, "statusLevel"));
            return cust;
        }

        @Override
        public Map<String, Value> toMap(Customer cust) {
            return MapUtil.buildMap()
                .add("customerId", cust.getCustomerId())
                .add("firstName", cust.getFirstName())
                .add("lastName", cust.getLastName())
                .add("email", cust.getEmail())
                .add("phone", cust.getPhone())
                .add("totalSpend", cust.getTotalSpendInCents())
                .add("statusLevel", cust.getStatusLevel())
                .done();
        }

        @Override
        public Object id(Customer cust) {
            return cust.getCustomerId();
        }
    }
    
    public static class Account {
        private String pan;
        private String customerId;
        private String expiryDate;
        private long balanceInCents;
        private long creditLimitInCents;
        private String status;

        public String getPan() { return pan; }
        public void setPan(String pan) { this.pan = pan; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getExpiryDate() { return expiryDate; }
        public void setExpiryDate(String expiryDate) { this.expiryDate = expiryDate; }
        public long getBalanceInCents() { return balanceInCents; }
        public void setBalanceInCents(long balanceInCents) { this.balanceInCents = balanceInCents; }
        public long getCreditLimitInCents() { return creditLimitInCents; }
        public void setCreditLimitInCents(long creditLimitInCents) { this.creditLimitInCents = creditLimitInCents; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }

    public static class AccountMapper implements RecordMapper<Account> {
        @Override
        public Account fromMap(Map<String, Object> map, Key recordKey, int generation) {
            Account acct = new Account();
            acct.setPan(MapUtil.asString(map, "pan"));
            acct.setCustomerId(MapUtil.asString(map, "customerId"));
            acct.setExpiryDate(MapUtil.asString(map, "expiryDate"));
            acct.setBalanceInCents(MapUtil.asLong(map, "balanceInCents"));
            acct.setCreditLimitInCents(MapUtil.asLong(map, "creditLimit"));
            acct.setStatus(MapUtil.asString(map, "status"));
            return acct;
        }

        @Override
        public Map<String, Value> toMap(Account acct) {
            return MapUtil.buildMap()
                .add("pan", acct.getPan())
                .add("customerId", acct.getCustomerId())
                .add("expiryDate", acct.getExpiryDate())
                .add("balanceInCents", acct.getBalanceInCents())
                .add("creditLimit", acct.getCreditLimitInCents())
                .add("status", acct.getStatus())
                .done();
        }

        @Override
        public Object id(Account acct) {
            return acct.getPan();
        }
    }
    
    public static void main(String[] args) {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);

            TypeSafeDataSet<Customer> customerDataSet = TypeSafeDataSet.of("test", "customers", Customer.class);
            TypeSafeDataSet<Account> accountDataSet = TypeSafeDataSet.of("test", "accounts", Account.class);
            TypeSafeDataSet<Transaction> txnDataSet = TypeSafeDataSet.of("test", "txns", Transaction.class);

            CustomerMapper customerMapper = new CustomerMapper();
            AccountMapper accountMapper = new AccountMapper();
            TransactionMapper txnMapper = new TransactionMapper();

            Customer customer = new Customer();
            customer.setCustomerId("CUST-10042");
            customer.setFirstName("Jane");
            customer.setLastName("Morrison");
            customer.setEmail("jane.morrison@example.com");
            customer.setPhone("+1-555-867-5309");
            customer.setTotalSpendInCents(0);
            customer.setStatusLevel("BRONZE");

            session.insert(customerDataSet)
                .object(customer)
                .using(customerMapper)
                .execute();

            Account account = new Account();
            account.setPan("4532015112830366");
            account.setCustomerId(customer.getCustomerId());
            account.setExpiryDate("03/28");
            account.setBalanceInCents(0);
            account.setCreditLimitInCents(500000);
            account.setStatus("ACTIVE");

            session.insert(accountDataSet)
                .object(account)
                .using(accountMapper)
                .execute();
            
            Transaction txn = new Transaction();
            txn.setId("TXN-00001");
            txn.setDesc("Car repairs");
            txn.setAmountInCents(45000);
            txn.setDate(System.currentTimeMillis());

            // ======================================================================
            // Old style: Standard Aerospike Java Client
            // ======================================================================
            //
            // Key txnKey = new Key("test", "txns", txn.getId());
            // Key accountKey = new Key("test", "accounts", account.getPan());
            // Key customerKey = new Key("test", "customers", customer.getCustomerId());
            //
            // BatchWritePolicy insertPolicy = new BatchWritePolicy();
            // insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            //
            // BatchWritePolicy updatePolicy = new BatchWritePolicy();
            // updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
            //
            // Expression statusExp = Exp.build(
            //     Exp.cond(
            //         Exp.gt(Exp.intBin("totalSpend"), Exp.val(100000)), Exp.val("PLATINUM"),
            //         Exp.gt(Exp.intBin("totalSpend"), Exp.val(10000)),  Exp.val("GOLD"),
            //         Exp.gt(Exp.intBin("totalSpend"), Exp.val(100)),    Exp.val("SILVER"),
            //         Exp.val("BRONZE")
            //     )
            // );
            //
            // List<BatchRecord> batchRecords = new ArrayList<>();
            //
            // batchRecords.add(new BatchWrite(
            //     insertPolicy, txnKey,
            //     Operation.put(new Bin("id", txn.getId())),
            //     Operation.put(new Bin("desc", txn.getDesc())),
            //     Operation.put(new Bin("amountInCents", txn.getAmountInCents())),
            //     Operation.put(new Bin("date", txn.getDate()))
            // ));
            //
            // batchRecords.add(new BatchWrite(
            //     updatePolicy, accountKey,
            //     Operation.add(new Bin("balanceCents", txn.getAmountInCents()))
            // ));
            //
            // batchRecords.add(new BatchWrite(
            //     updatePolicy, customerKey,
            //     Operation.add(new Bin("totalSpend", txn.getAmountInCents())),
            //     ExpOperation.write("statusLevel", statusExp, ExpWriteFlags.DEFAULT)
            // ));
            //
            // client.operate(new BatchPolicy(), batchRecords);

            // ======================================================================
            // New style: Fluent API
            // ======================================================================
            session
                .insert(txnDataSet)
                    .object(txn)
                    .using(txnMapper)
                .update(accountDataSet.id(account.getPan()))
                    .bin("balanceCents").add(txn.getAmountInCents())
                .update(customerDataSet.id(customer.getCustomerId()))
                    .bin("totalSpend").add(txn.getAmountInCents())
                    .bin("statusLevel").upsertFrom("when ($.totalSpend > 100000 => 'PLATINUM', "
                            + "$.totalSpend > 10000 => 'GOLD', "
                            + "$.totalSpend > 100 => 'SILVER', "
                            + "default => 'BRONZE')")
                .execute();
        }
    }
}
