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
package com.aerospike.examples.ecommerce;

public class Order {
    private String orderId;
    private String customerId;
    private String sku;
    private int qty;
    private long totalCents;
    private String status;
    private long timestamp;

    public Order() {}

    public Order(String orderId, String customerId, String sku, int qty,
                 long totalCents, String status, long timestamp) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.sku = sku;
        this.qty = qty;
        this.totalCents = totalCents;
        this.status = status;
        this.timestamp = timestamp;
    }

    public String getOrderId()      { return orderId; }
    public String getCustomerId()   { return customerId; }
    public String getSku()          { return sku; }
    public int getQty()             { return qty; }
    public long getTotalCents()     { return totalCents; }
    public String getStatus()       { return status; }
    public long getTimestamp()       { return timestamp; }

    public void setOrderId(String id)           { this.orderId = id; }
    public void setCustomerId(String id)        { this.customerId = id; }
    public void setSku(String sku)              { this.sku = sku; }
    public void setQty(int qty)                 { this.qty = qty; }
    public void setTotalCents(long total)        { this.totalCents = total; }
    public void setStatus(String status)        { this.status = status; }
    public void setTimestamp(long ts)            { this.timestamp = ts; }

    @Override
    public String toString() {
        return String.format("Order[%s, customer=%s, sku=%s, qty=%d, $%.2f, %s]",
            orderId, customerId, sku, qty, totalCents / 100.0, status);
    }
}
