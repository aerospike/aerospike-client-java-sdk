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

public class Product {
    private String sku;
    private String name;
    private long priceCents;
    private int stockQty;
    private long salePriceCents;

    public Product() {}

    public Product(String sku, String name, long priceCents, int stockQty) {
        this.sku = sku;
        this.name = name;
        this.priceCents = priceCents;
        this.stockQty = stockQty;
        this.salePriceCents = 0;
    }

    public String getSku()            { return sku; }
    public String getName()           { return name; }
    public long getPriceCents()       { return priceCents; }
    public int getStockQty()          { return stockQty; }
    public long getSalePriceCents()   { return salePriceCents; }

    public void setSku(String sku)                  { this.sku = sku; }
    public void setName(String name)                { this.name = name; }
    public void setPriceCents(long price)            { this.priceCents = price; }
    public void setStockQty(int qty)                { this.stockQty = qty; }
    public void setSalePriceCents(long salePrice)    { this.salePriceCents = salePrice; }

    public boolean isOnSale() { return salePriceCents > 0; }

    @Override
    public String toString() {
        if (isOnSale()) {
            return String.format("Product[%s, %s, $%.2f -> SALE $%.2f, stock=%d]",
                sku, name, priceCents / 100.0, salePriceCents / 100.0, stockQty);
        }
        return String.format("Product[%s, %s, $%.2f, stock=%d]",
            sku, name, priceCents / 100.0, stockQty);
    }
}
