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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.ErrorStrategy;
import com.aerospike.client.fluent.Key;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.TypeSafeDataSet;
import com.aerospike.client.fluent.policy.Behavior;
import com.aerospike.client.fluent.task.ExecuteTask;

/**
 * E-commerce order fulfillment example demonstrating CompletableFuture chaining,
 * Flow.Publisher streaming, and error handling with the Aerospike Fluent API.
 *
 * <p>Scenario: A customer places an order. We verify the customer exists, check
 * product stock, create the order, and decrement inventory -- all composed with
 * CompletableFuture. Then we scan products using Flow.Publisher with backpressure.</p>
 */
public class EcommerceExample {

    static final CustomerMapper CUSTOMER_MAPPER = new CustomerMapper();
    static final ProductMapper  PRODUCT_MAPPER  = new ProductMapper();
    static final OrderMapper    ORDER_MAPPER    = new OrderMapper();

    public static void main(String[] args) throws Exception {
        try (Cluster cluster = new ClusterDefinition("localhost", 3100).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);

            TypeSafeDataSet<Customer> customers = TypeSafeDataSet.of("test", "customers", Customer.class);
            TypeSafeDataSet<Product>  products  = TypeSafeDataSet.of("test", "products",  Product.class);
            TypeSafeDataSet<Order>    orders    = TypeSafeDataSet.of("test", "orders",    Order.class);

            // ==========================================
            // 1. Seed 20 customers, 100 products, and
            //    54 orders into Aerospike
            // ==========================================
            SeedData.seed(session, customers, products, orders); 

            // ==========================================
            // 2. Place an order using CompletableFuture
            //    (async lookup -> validate -> batch write)
            // ==========================================
            placeOrder(session, customers, products, orders, "C-100", "SKU-LAP01", 1);

            // ==========================================
            // 3. Demonstrate error handling on a
            //    non-existent customer
            // ==========================================
            placeOrderWithErrorHandling(session, customers, products, orders,
                    "C-MISSING", "SKU-LAP01", 1);

            // ==========================================
            // 4. Stream orders for a customer using
            //    Flow.Publisher with backpressure
            // ==========================================
            streamOrders(session, orders, "C-100");

            // ==========================================
            // 5. Batch query: top-spender dashboard
            // ==========================================
            topSpenderDashboard(session, customers, orders);

            // ==========================================
            // 6. Map operations: product ratings
            // ==========================================
            productRatings(session, products);

            // ==========================================
            // 7. Scan for affordable, well-stocked
            //    products using Flow.Publisher
            // ==========================================
            scanAffordableProducts(session, products);

            // ==========================================
            // 8. Background scan: apply sale prices to
            //    overstocked, cheap products
            // ==========================================
            applySalePrices(session, products);

            // ==========================================
            // 9. Re-display stock after sale prices
            // ==========================================
            scanAffordableProducts(session, products);
        }
    }

    // ------------------------------------------------------------------
    // Place an order composed with CompletableFuture:
    //   lookup customer -> lookup product -> create order + decrement stock
    // ------------------------------------------------------------------
    static void placeOrder(Session session,
                           TypeSafeDataSet<Customer> customers,
                           TypeSafeDataSet<Product> products,
                           TypeSafeDataSet<Order> orders,
                           String customerId, String sku, int qty) throws Exception {

        System.out.println("--- Placing order: customer=" + customerId
                + ", sku=" + sku + ", qty=" + qty + " ---");

        // Step A: Fetch the customer asynchronously
        CompletableFuture<Customer> customerFuture = session
                .query(customers.id(customerId))
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFuture(CUSTOMER_MAPPER)
                .thenApply(List::getFirst);

        // Step B: Fetch the product asynchronously (runs in parallel with Step A)
        CompletableFuture<Product> productFuture = session
                .query(products.id(sku))
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFuture(PRODUCT_MAPPER)
                .thenApply(List::getFirst);

        // Step C: When both complete, validate and create the order
        Order order = customerFuture
                .thenCombine(productFuture, (cust, prod) -> {
                    if (prod.getStockQty() < qty) {
                        throw new IllegalStateException(
                                "Insufficient stock for " + prod.getName()
                                        + " (available: " + prod.getStockQty() + ")");
                    }
                    long total = prod.getPriceCents() * qty;
                    return new Order("ORD-2001", cust.getId(), prod.getSku(), qty,
                            total, "CONFIRMED", System.currentTimeMillis());
                })
                // Step D: Persist the order and decrement stock in a single batch
                .thenCompose(ord -> {
                    session.insert(orders)
                            .object(ord).using(ORDER_MAPPER)
                        .update(products.id(ord.getSku()))
                            .bin("stock").add(-ord.getQty())
                        .update(customers.id(ord.getCustomerId()))
                            .bin("balance").add(ord.getTotalCents())
                        .execute();
                    return CompletableFuture.completedFuture(ord);
                })
                .join();

        System.out.println("Order placed: " + order);

        // Verify updated stock
        Product updated = session.query(products.id(sku))
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFuture(PRODUCT_MAPPER).join().getFirst();
        System.out.println("Updated product: " + updated);
        System.out.println();
    }

    // ------------------------------------------------------------------
    // Demonstrate error handling: querying a non-existent customer
    // ------------------------------------------------------------------
    static void placeOrderWithErrorHandling(Session session,
                                            TypeSafeDataSet<Customer> customers,
                                            TypeSafeDataSet<Product> products,
                                            TypeSafeDataSet<Order> orders,
                                            String customerId, String sku, int qty) {

        System.out.println("--- Attempting order for non-existent customer: "
                + customerId + " ---");

        // Option A: Async CompletableFuture with exceptionally()
        session.query(customers.id(customerId))
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFuture(CUSTOMER_MAPPER)
                .thenApply(list -> {
                    if (list.isEmpty()) {
                        throw new IllegalStateException("Customer not found: " + customerId);
                    }
                    return list.getFirst();
                })
                .thenAccept(cust -> System.out.println("Found: " + cust))
                .exceptionally(ex -> {
                    System.out.println("Expected error: " + ex.getMessage());
                    return null;
                })
                .join();

        // Option B: ErrorHandler callback -- errors go to the lambda, successes to the stream
        System.out.println("\nUsing ErrorHandler callback:");
        RecordStream rs = session.query(
                customers.ids("C-100", "C-MISSING", "C-ALSO-MISSING"))
                .includeMissingKeys()
                .execute((key, index, ex) ->
                        System.out.println("  Error at index " + index + " for key "
                                + key + ": " + ex.getMessage()));
        rs.forEach(rr -> System.out.println("  OK: " + rr.key()));

        // Option C: IN_STREAM strategy -- check each result individually
        System.out.println("\nUsing ErrorStrategy.IN_STREAM:");
        RecordStream inStream = session.query(
                customers.ids("C-100", "C-MISSING"))
                .includeMissingKeys()
                .execute(ErrorStrategy.IN_STREAM);
        inStream.forEach(rr -> {
            if (rr.isOk()) {
                System.out.println("  OK:    " + rr.key());
            } else {
                System.out.println("  Error: " + rr.key() + " -> " + rr.message());
            }
        });
        System.out.println();
    }

    // ------------------------------------------------------------------
    // Stream all orders for a given customer using Flow.Publisher
    // ------------------------------------------------------------------
    static void streamOrders(Session session,
                             TypeSafeDataSet<Order> orders,
                             String customerId) throws InterruptedException {

        System.out.println("--- Streaming orders for customer " + customerId
                + " via Flow.Publisher ---");

        Flow.Publisher<RecordResult> publisher = session
                .query(orders)
                .where("$.customerId == '" + customerId + "'")
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asPublisher();

        CountDownLatch latch = new CountDownLatch(1);

        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription sub) {
                this.subscription = sub;
                sub.request(5);
            }

            @Override
            public void onNext(RecordResult item) {
                if (item.isOk()) {
                    Order o = ORDER_MAPPER.fromMap(
                            item.recordOrThrow().bins, item.key(),
                            item.recordOrThrow().generation);
                    System.out.println("  Received: " + o);
                } else {
                    System.out.println("  Error: " + item.message());
                }
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("  Publisher error: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onComplete() {
                System.out.println("  Stream complete.");
                latch.countDown();
            }
        });

        latch.await();
        System.out.println();
    }

    // ------------------------------------------------------------------
    // Batch query + where clause: look up 5 customers, then for each
    // one query the orders set to find their orders -- all async.
    // ------------------------------------------------------------------
    static void topSpenderDashboard(Session session,
                                    TypeSafeDataSet<Customer> customers,
                                    TypeSafeDataSet<Order> orders) throws Exception {

        System.out.println("--- Top-spender dashboard (batch query + where clause) ---");

        // Batch-fetch 5 customers in a single async round-trip
        List<Key> topKeys = List.of(
                customers.id("C-103"), customers.id("C-107"),
                customers.id("C-110"), customers.id("C-112"),
                customers.id("C-117"));

        List<Customer> topCustomers = session
                .query(topKeys)
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asCompletableFuture(CUSTOMER_MAPPER)
                .join();

        // For each customer, query the orders set using a where clause
        // to find their orders -- all 5 queries launched in parallel
        List<CompletableFuture<List<Order>>> orderFutures = topCustomers.stream()
                .map(c -> session.query(orders)
                        .where("$.customerId == '" + c.getId() + "'")
                        .executeAsync(ErrorStrategy.IN_STREAM)
                        .asCompletableFuture(ORDER_MAPPER))
                .toList();

        // Wait for all order queries to complete
        CompletableFuture.allOf(orderFutures.toArray(CompletableFuture[]::new)).join();

        for (int i = 0; i < topCustomers.size(); i++) {
            Customer c = topCustomers.get(i);
            List<Order> custOrders = orderFutures.get(i).join();
            long totalSpent = custOrders.stream().mapToLong(Order::getTotalCents).sum();
            System.out.printf("  %-18s  balance=$%8.2f  orders=%d  order_total=$%.2f%n",
                    c.getName(), c.getBalanceCents() / 100.0,
                    custOrders.size(), totalSpent / 100.0);
        }
        System.out.println();
    }

    // ------------------------------------------------------------------
    // Map operations: store and query per-customer product ratings
    // using Aerospike CDT map operations on a "ratings" bin.
    //
    //   ratings bin = { "C-100": 5, "C-103": 4, "C-107": 3, ... }
    // ------------------------------------------------------------------
    static void productRatings(Session session,
                               TypeSafeDataSet<Product> products) {

        System.out.println("--- Map operations: product ratings for SKU-TV55 ---");
        Key tvKey = products.id("SKU-TV55");

        // 1. Add ratings from several customers in one atomic operation
        session.upsert(tvKey)
                .bin("ratings").onMapKey("C-100").setTo(5L)
                .bin("ratings").onMapKey("C-101").setTo(4L)
                .bin("ratings").onMapKey("C-103").setTo(4L)
                .bin("ratings").onMapKey("C-107").setTo(3L)
                .bin("ratings").onMapKey("C-112").setTo(5L)
                .bin("ratings").onMapKey("C-110").setTo(2L)
                .bin("ratings").onMapKey("C-117").setTo(4L)
                .execute();
        System.out.println("  Added 7 ratings.");

        // 2. Read a specific customer's rating
        RecordResult aliceRating = session.query(tvKey)
                .bin("ratings").onMapKey("C-100").getValues()
                .execute()
                .getFirst().orElseThrow();
        System.out.println("  Alice's rating: " + aliceRating.recordOrThrow().getLong("ratings"));

        // 3. Get the highest-rated entry (rank -1 = highest value)
        RecordResult topRated = session.query(tvKey)
                .bin("ratings").onMapRank(-1).getKeysAndValues()
                .execute()
                .getFirst().orElseThrow();
        System.out.println("  Highest rating entry: " + topRated.recordOrThrow().bins.get("ratings"));

        // 4. Count how many 4-and-5-star ratings (value range [4, 6) )
        RecordResult highRatings = session.query(tvKey)
                .bin("ratings").onMapValueRange(4L, 6L).count()
                .execute()
                .getFirst().orElseThrow();
        System.out.println("  4+ star ratings: " + highRatings.recordOrThrow().getLong("ratings"));

        // 5. Update a rating (customer C-107 changes their mind: 3 -> 5)
        session.upsert(tvKey)
                .bin("ratings").onMapKey("C-107").setTo(5L)
                .execute();
        System.out.println("  Updated C-107's rating to 5.");

        // 6. Remove a rating (customer C-110 deletes their review)
        session.upsert(tvKey)
                .bin("ratings").onMapKey("C-110").remove()
                .execute();
        System.out.println("  Removed C-110's rating.");

        // 7. Get remaining count of 4+ star ratings after changes
        RecordResult updatedHighRatings = session.query(tvKey)
                .bin("ratings").onMapValueRange(4L, 6L).count()
                .execute()
                .getFirst().orElseThrow();
        System.out.println("  4+ star ratings after update: "
                + updatedHighRatings.recordOrThrow().getLong("ratings"));

        System.out.println();
    }

    // ------------------------------------------------------------------
    // Background scan: find overstocked cheap products and apply a sale
    // price using a server-side expression.
    //
    //   stock > 250 AND price <= $50 (5000 cents):
    //     - price >= $10 (1000 cents) -> sale = 80% of price
    //     - price <  $10              -> sale = 90% of price
    // ------------------------------------------------------------------
    static void applySalePrices(Session session,
                                TypeSafeDataSet<Product> products) {

        System.out.println("--- Background scan: applying sale prices "
                + "(stock > 250, price <= $50) ---");

        ExecuteTask task = session.backgroundTask().update(products)
                .where("$.stock > 250 and $.price <= 5000")
                .bin("salePrice").upsertFrom(
                        "when ($.price >= 1000 => $.price * 8 / 10, "
                        + "default => $.price * 9 / 10)")
                .execute();

        task.waitTillComplete();
        System.out.println("  Sale prices applied.\n");
    }

    // ------------------------------------------------------------------
    // Scan the products set for items with stock > 100 and price < $100,
    // streaming results through a Flow.Publisher with backpressure.
    // ------------------------------------------------------------------
    static void scanAffordableProducts(Session session,
                                       TypeSafeDataSet<Product> products)
            throws InterruptedException {

        System.out.println("--- Scanning for products: stock > 100 AND price < $100 ---");

        Flow.Publisher<RecordResult> publisher = session
                .query(products)
                .where("$.stock > 100 and $.price < 10000")
                .executeAsync(ErrorStrategy.IN_STREAM)
                .asPublisher();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();

        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription sub) {
                this.subscription = sub;
                sub.request(10);
            }

            @Override
            public void onNext(RecordResult item) {
                if (item.isOk()) {
                    Product p = PRODUCT_MAPPER.fromMap(
                            item.recordOrThrow().bins, item.key(),
                            item.recordOrThrow().generation);
                    String sale = p.isOnSale()
                            ? String.format("SALE $%.2f", p.getSalePriceCents() / 100.0)
                            : "";
                    System.out.printf("  [%2d] %-35s $%6.2f  stock=%-3d  %s%n",
                            count.incrementAndGet(), p.getName(),
                            p.getPriceCents() / 100.0, p.getStockQty(), sale);
                }
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("  Scan error: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onComplete() {
                System.out.println("  Scan complete: " + count.get()
                        + " matching products found.");
                latch.countDown();
            }
        });

        latch.await();
        System.out.println();
    }
}
