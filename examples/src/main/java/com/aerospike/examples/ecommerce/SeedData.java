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

import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.TypeSafeDataSet;

/**
 * Seeds the Aerospike database with 20 customers, 100 products, and a realistic
 * spread of orders across customers.
 */
public class SeedData {

    private SeedData() {}

    static final CustomerMapper CUSTOMER_MAPPER = new CustomerMapper();
    static final ProductMapper  PRODUCT_MAPPER  = new ProductMapper();
    static final OrderMapper    ORDER_MAPPER    = new OrderMapper();

    // -----------------------------------------------------------------
    // Customers
    // -----------------------------------------------------------------
    private static final List<Customer> CUSTOMERS = List.of(
        new Customer("C-100", "Alice Park",       "alice@example.com",         500_000,         0),
        new Customer("C-101", "Bob Chen",         "bob.chen@example.com",      250_000,     8_499),
        new Customer("C-102", "Carol Martinez",   "carol.m@example.com",       100_000,    52_300),
        new Customer("C-103", "David Kim",        "dkim@example.com",          750_000,   124_000),
        new Customer("C-104", "Eva Johnson",      "eva.j@example.com",         300_000,     3_200),
        new Customer("C-105", "Frank Liu",        "frank.liu@example.com",     400_000,    97_550),
        new Customer("C-106", "Grace Patel",      "grace.p@example.com",       200_000,    15_800),
        new Customer("C-107", "Henry Nguyen",     "henry.n@example.com",       600_000,   210_000),
        new Customer("C-108", "Isabel Torres",    "isabel.t@example.com",      350_000,    44_200),
        new Customer("C-109", "Jack Wilson",      "jack.w@example.com",        150_000,     1_000),
        new Customer("C-110", "Karen Zhang",      "karen.z@example.com",       500_000,   180_750),
        new Customer("C-111", "Leo Brown",        "leo.b@example.com",         100_000,    67_300),
        new Customer("C-112", "Mia Garcia",       "mia.g@example.com",         800_000,   340_000),
        new Customer("C-113", "Noah Davis",       "noah.d@example.com",        200_000,    22_100),
        new Customer("C-114", "Olivia Lee",       "olivia.l@example.com",      450_000,    89_400),
        new Customer("C-115", "Paul Robinson",    "paul.r@example.com",        300_000,     5_600),
        new Customer("C-116", "Quinn Miller",     "quinn.m@example.com",       250_000,    38_900),
        new Customer("C-117", "Ruby Anderson",    "ruby.a@example.com",        550_000,   155_200),
        new Customer("C-118", "Sam Thomas",       "sam.t@example.com",         175_000,    11_750),
        new Customer("C-119", "Tina Jackson",     "tina.j@example.com",        400_000,    72_600)
    );

    // -----------------------------------------------------------------
    // Products (100 items across categories)
    // -----------------------------------------------------------------
    private static final List<Product> PRODUCTS = List.of(
        // Electronics
        new Product("SKU-TV55",    "55\" 4K Smart TV",             49_999,  20),
        new Product("SKU-TV65",    "65\" OLED TV",                 129_999,   8),
        new Product("SKU-HP01",    "Wireless Headphones",           7_999, 150),
        new Product("SKU-HP02",    "Noise-Cancelling Headphones",  24_999,  60),
        new Product("SKU-SPK01",   "Bluetooth Speaker",             3_499, 200),
        new Product("SKU-SPK02",   "Soundbar",                     19_999,  35),
        new Product("SKU-TAB01",   "10\" Tablet",                  29_999,  45),
        new Product("SKU-TAB02",   "12\" Pro Tablet",              79_999,  15),
        new Product("SKU-LAP01",   "Ultrabook Laptop",             89_999,  25),
        new Product("SKU-LAP02",   "Gaming Laptop",               149_999,  10),
        new Product("SKU-PHN01",   "Smartphone 128GB",             69_999,  40),
        new Product("SKU-PHN02",   "Smartphone 256GB",             89_999,  30),
        new Product("SKU-CAM01",   "Mirrorless Camera",            99_999,  12),
        new Product("SKU-CAM02",   "Action Camera",                29_999,  80),
        new Product("SKU-DRN01",   "Camera Drone",                 59_999,  18),
        // Home & Kitchen
        new Product("SKU-CFM01",   "Espresso Machine",             34_999,  50),
        new Product("SKU-CFM02",   "Drip Coffee Maker",             4_999, 120),
        new Product("SKU-BLN01",   "High-Speed Blender",            8_999,  90),
        new Product("SKU-AIR01",   "Air Fryer",                     6_999, 110),
        new Product("SKU-TOA01",   "Toaster Oven",                  5_499, 130),
        new Product("SKU-MIX01",   "Stand Mixer",                  27_999,  40),
        new Product("SKU-VAC01",   "Robot Vacuum",                 39_999,  55),
        new Product("SKU-VAC02",   "Cordless Vacuum",              24_999,  70),
        new Product("SKU-PUR01",   "Air Purifier",                 14_999,  85),
        new Product("SKU-HUM01",   "Humidifier",                    3_999, 160),
        // Fitness & Outdoors
        new Product("SKU-BIK01",   "Mountain Bike",                54_999,  15),
        new Product("SKU-BIK02",   "Road Bike",                    79_999,   8),
        new Product("SKU-TRD01",   "Treadmill",                    69_999,  12),
        new Product("SKU-YGA01",   "Yoga Mat Premium",              2_999, 300),
        new Product("SKU-DUM01",   "Adjustable Dumbbell Set",      24_999,  65),
        new Product("SKU-TNT01",   "4-Person Tent",                12_999,  40),
        new Product("SKU-SLP01",   "Sleeping Bag",                  4_999, 180),
        new Product("SKU-HKB01",   "Hiking Boots",                  8_999, 120),
        new Product("SKU-KYK01",   "Inflatable Kayak",             19_999,  22),
        new Product("SKU-FIT01",   "Fitness Tracker",               4_999, 250),
        // Clothing
        new Product("SKU-JKT01",   "Waterproof Jacket",             7_999, 140),
        new Product("SKU-JKT02",   "Down Parka",                   14_999,  75),
        new Product("SKU-SNK01",   "Running Shoes",                 9_999, 200),
        new Product("SKU-SNK02",   "Trail Running Shoes",          11_999,  90),
        new Product("SKU-TSH01",   "Performance T-Shirt",           1_999, 500),
        new Product("SKU-TSH02",   "Graphic Tee",                   1_499, 400),
        new Product("SKU-JNS01",   "Slim Fit Jeans",                3_999, 250),
        new Product("SKU-JNS02",   "Relaxed Fit Jeans",             3_499, 280),
        new Product("SKU-DRS01",   "Casual Dress",                  4_999, 150),
        new Product("SKU-HAT01",   "Baseball Cap",                    999, 600),
        // Books & Media
        new Product("SKU-BK001",   "Java Concurrency In Practice",  3_499, 350),
        new Product("SKU-BK002",   "Designing Data-Intensive Apps",  4_299, 280),
        new Product("SKU-BK003",   "Clean Code",                    3_199, 400),
        new Product("SKU-BK004",   "The Pragmatic Programmer",      3_999, 320),
        new Product("SKU-BK005",   "Database Internals",            4_999, 200),
        new Product("SKU-VG001",   "Strategy Game Deluxe",          5_999, 100),
        new Product("SKU-VG002",   "Racing Sim",                    4_999,  80),
        new Product("SKU-VG003",   "RPG Adventure",                 5_999,  90),
        new Product("SKU-VG004",   "Puzzle Collection",             1_999, 220),
        new Product("SKU-VG005",   "Sports Game 2026",              5_999,  60),
        // Office & Tech Accessories
        new Product("SKU-MNT01",   "27\" 4K Monitor",              34_999,  30),
        new Product("SKU-MNT02",   "Ultrawide Monitor",            49_999,  18),
        new Product("SKU-KBD01",   "Mechanical Keyboard",           9_999, 110),
        new Product("SKU-KBD02",   "Ergonomic Keyboard",           12_999,  70),
        new Product("SKU-MSE01",   "Wireless Mouse",                3_999, 300),
        new Product("SKU-MSE02",   "Gaming Mouse",                  5_999, 140),
        new Product("SKU-WBC01",   "4K Webcam",                     7_999, 160),
        new Product("SKU-USB01",   "USB-C Hub",                     3_499, 250),
        new Product("SKU-CHG01",   "65W USB-C Charger",             2_999, 350),
        new Product("SKU-SSD01",   "1TB Portable SSD",              7_999,  95),
        new Product("SKU-SSD02",   "2TB Portable SSD",             12_999,  50),
        new Product("SKU-HDD01",   "4TB External HDD",              8_999,  75),
        new Product("SKU-PWR01",   "20000mAh Power Bank",           2_999, 270),
        new Product("SKU-CBL01",   "USB-C Cable 3-Pack",              999, 800),
        new Product("SKU-ADP01",   "Universal Travel Adapter",      1_999, 400),
        // Pet Supplies
        new Product("SKU-PET01",   "Automatic Pet Feeder",          5_999, 120),
        new Product("SKU-PET02",   "Pet Camera",                    4_999,  90),
        new Product("SKU-PET03",   "Dog Bed Large",                 3_999, 200),
        new Product("SKU-PET04",   "Cat Tower Deluxe",              7_999,  80),
        new Product("SKU-PET05",   "Pet Grooming Kit",              2_499, 300),
        // Garden
        new Product("SKU-GRD01",   "Cordless Lawn Mower",          29_999,  25),
        new Product("SKU-GRD02",   "Garden Tool Set",               3_499, 180),
        new Product("SKU-GRD03",   "LED Solar Lights 10-Pack",      2_499, 350),
        new Product("SKU-GRD04",   "Portable Fire Pit",             9_999,  45),
        new Product("SKU-GRD05",   "Patio Umbrella",                5_999,  60),
        // Health & Personal Care
        new Product("SKU-HLT01",   "Electric Toothbrush",           4_999, 180),
        new Product("SKU-HLT02",   "Digital Scale",                 2_499, 250),
        new Product("SKU-HLT03",   "Blood Pressure Monitor",        3_999, 130),
        new Product("SKU-HLT04",   "Massage Gun",                  12_999,  55),
        new Product("SKU-HLT05",   "First Aid Kit",                 1_999, 400),
        // Toys & Games
        new Product("SKU-TOY01",   "Building Blocks 1000pc",        3_999, 250),
        new Product("SKU-TOY02",   "RC Car",                        4_999, 110),
        new Product("SKU-TOY03",   "Board Game Collection",         2_999, 300),
        new Product("SKU-TOY04",   "Science Kit for Kids",          2_499, 180),
        new Product("SKU-TOY05",   "Jigsaw Puzzle 2000pc",          1_499, 220),
        // Travel
        new Product("SKU-TRV01",   "Carry-On Suitcase",            12_999,  60),
        new Product("SKU-TRV02",   "Packing Cube Set",              1_999, 350),
        new Product("SKU-TRV03",   "Neck Pillow Memory Foam",       1_499, 500),
        new Product("SKU-TRV04",   "Luggage Scale",                   999, 400),
        new Product("SKU-TRV05",   "Waterproof Dry Bag",            1_499, 280)
    );

    // -----------------------------------------------------------------
    // Orders -- spread across customers so each has at least one
    // -----------------------------------------------------------------
    private static final List<Order> ORDERS;

    static {
        long now = System.currentTimeMillis();
        long day = 86_400_000L;
        ORDERS = List.of(
            // Alice (C-100): 3 orders
            new Order("ORD-1001", "C-100", "SKU-TV55",   1,  49_999, "CONFIRMED", now - 10 * day),
            new Order("ORD-1002", "C-100", "SKU-HP01",   2,  15_998, "SHIPPED",   now - 8 * day),
            new Order("ORD-1003", "C-100", "SKU-BK001",  1,   3_499, "DELIVERED", now - 3 * day),
            // Bob (C-101): 2 orders
            new Order("ORD-1004", "C-101", "SKU-CFM02",  1,   4_999, "DELIVERED", now - 15 * day),
            new Order("ORD-1005", "C-101", "SKU-TSH01",  2,   3_998, "CONFIRMED", now - 1 * day),
            // Carol (C-102): 4 orders
            new Order("ORD-1006", "C-102", "SKU-LAP01",  1,  89_999, "DELIVERED", now - 30 * day),
            new Order("ORD-1007", "C-102", "SKU-KBD01",  1,   9_999, "DELIVERED", now - 25 * day),
            new Order("ORD-1008", "C-102", "SKU-MSE01",  1,   3_999, "SHIPPED",   now - 5 * day),
            new Order("ORD-1009", "C-102", "SKU-SSD01",  1,   7_999, "CONFIRMED", now - 1 * day),
            // David (C-103): 5 orders (high spender)
            new Order("ORD-1010", "C-103", "SKU-TV65",   1, 129_999, "DELIVERED", now - 60 * day),
            new Order("ORD-1011", "C-103", "SKU-LAP02",  1, 149_999, "DELIVERED", now - 45 * day),
            new Order("ORD-1012", "C-103", "SKU-CAM01",  1,  99_999, "SHIPPED",   now - 10 * day),
            new Order("ORD-1013", "C-103", "SKU-DRN01",  1,  59_999, "CONFIRMED", now - 2 * day),
            new Order("ORD-1014", "C-103", "SKU-MNT02",  1,  49_999, "CONFIRMED", now),
            // Eva (C-104): 1 order
            new Order("ORD-1015", "C-104", "SKU-YGA01",  1,   2_999, "DELIVERED", now - 20 * day),
            // Frank (C-105): 3 orders
            new Order("ORD-1016", "C-105", "SKU-BIK01",  1,  54_999, "DELIVERED", now - 40 * day),
            new Order("ORD-1017", "C-105", "SKU-HKB01",  1,   8_999, "SHIPPED",   now - 7 * day),
            new Order("ORD-1018", "C-105", "SKU-FIT01",  1,   4_999, "CONFIRMED", now - 1 * day),
            // Grace (C-106): 2 orders
            new Order("ORD-1019", "C-106", "SKU-PUR01",  1,  14_999, "DELIVERED", now - 12 * day),
            new Order("ORD-1020", "C-106", "SKU-HUM01",  2,   7_998, "SHIPPED",   now - 3 * day),
            // Henry (C-107): 4 orders (big spender)
            new Order("ORD-1021", "C-107", "SKU-TAB02",  1,  79_999, "DELIVERED", now - 50 * day),
            new Order("ORD-1022", "C-107", "SKU-PHN02",  2, 179_998, "DELIVERED", now - 30 * day),
            new Order("ORD-1023", "C-107", "SKU-SPK02",  1,  19_999, "SHIPPED",   now - 5 * day),
            new Order("ORD-1024", "C-107", "SKU-WBC01",  1,   7_999, "CONFIRMED", now),
            // Isabel (C-108): 2 orders
            new Order("ORD-1025", "C-108", "SKU-VAC01",  1,  39_999, "DELIVERED", now - 18 * day),
            new Order("ORD-1026", "C-108", "SKU-AIR01",  1,   6_999, "CONFIRMED", now - 2 * day),
            // Jack (C-109): 1 order
            new Order("ORD-1027", "C-109", "SKU-HAT01",  3,   2_997, "DELIVERED", now - 5 * day),
            // Karen (C-110): 4 orders
            new Order("ORD-1028", "C-110", "SKU-BIK02",  1,  79_999, "DELIVERED", now - 35 * day),
            new Order("ORD-1029", "C-110", "SKU-JKT01",  1,   7_999, "DELIVERED", now - 20 * day),
            new Order("ORD-1030", "C-110", "SKU-SNK01",  2,  19_998, "SHIPPED",   now - 4 * day),
            new Order("ORD-1031", "C-110", "SKU-DRS01",  1,   4_999, "CONFIRMED", now),
            // Leo (C-111): 2 orders
            new Order("ORD-1032", "C-111", "SKU-VG001",  1,   5_999, "DELIVERED", now - 22 * day),
            new Order("ORD-1033", "C-111", "SKU-VG003",  1,   5_999, "SHIPPED",   now - 3 * day),
            // Mia (C-112): 6 orders (top spender)
            new Order("ORD-1034", "C-112", "SKU-TV65",   1, 129_999, "DELIVERED", now - 55 * day),
            new Order("ORD-1035", "C-112", "SKU-LAP02",  1, 149_999, "DELIVERED", now - 40 * day),
            new Order("ORD-1036", "C-112", "SKU-PHN02",  1,  89_999, "DELIVERED", now - 25 * day),
            new Order("ORD-1037", "C-112", "SKU-MNT01",  1,  34_999, "SHIPPED",   now - 8 * day),
            new Order("ORD-1038", "C-112", "SKU-KBD02",  1,  12_999, "SHIPPED",   now - 3 * day),
            new Order("ORD-1039", "C-112", "SKU-CHG01",  3,   8_997, "CONFIRMED", now),
            // Noah (C-113): 1 order
            new Order("ORD-1040", "C-113", "SKU-BK002",  1,   4_299, "DELIVERED", now - 10 * day),
            // Olivia (C-114): 3 orders
            new Order("ORD-1041", "C-114", "SKU-TRD01",  1,  69_999, "DELIVERED", now - 28 * day),
            new Order("ORD-1042", "C-114", "SKU-DUM01",  1,  24_999, "SHIPPED",   now - 6 * day),
            new Order("ORD-1043", "C-114", "SKU-TSH01",  3,   5_997, "CONFIRMED", now - 1 * day),
            // Paul (C-115): 1 order
            new Order("ORD-1044", "C-115", "SKU-TOY01",  2,   7_998, "SHIPPED",   now - 4 * day),
            // Quinn (C-116): 2 orders
            new Order("ORD-1045", "C-116", "SKU-PET01",  1,   5_999, "DELIVERED", now - 14 * day),
            new Order("ORD-1046", "C-116", "SKU-PET03",  1,   3_999, "CONFIRMED", now - 1 * day),
            // Ruby (C-117): 4 orders
            new Order("ORD-1047", "C-117", "SKU-CAM01",  1,  99_999, "DELIVERED", now - 38 * day),
            new Order("ORD-1048", "C-117", "SKU-SSD02",  1,  12_999, "DELIVERED", now - 20 * day),
            new Order("ORD-1049", "C-117", "SKU-GRD01",  1,  29_999, "SHIPPED",   now - 5 * day),
            new Order("ORD-1050", "C-117", "SKU-HLT04",  1,  12_999, "CONFIRMED", now),
            // Sam (C-118): 1 order
            new Order("ORD-1051", "C-118", "SKU-TRV01",  1,  12_999, "SHIPPED",   now - 6 * day),
            // Tina (C-119): 3 orders
            new Order("ORD-1052", "C-119", "SKU-CFM01",  1,  34_999, "DELIVERED", now - 25 * day),
            new Order("ORD-1053", "C-119", "SKU-BLN01",  1,   8_999, "DELIVERED", now - 15 * day),
            new Order("ORD-1054", "C-119", "SKU-GRD03",  2,   4_998, "CONFIRMED", now - 2 * day)
        );
    }

    // -----------------------------------------------------------------
    // Bulk-insert all seed data
    // -----------------------------------------------------------------
    static void seed(Session session,
                     TypeSafeDataSet<Customer> customers,
                     TypeSafeDataSet<Product> products,
                     TypeSafeDataSet<Order> orders) {

        System.out.println("Seeding " + CUSTOMERS.size() + " customers, "
                + PRODUCTS.size() + " products, "
                + ORDERS.size() + " orders ...");

        long now = System.nanoTime();
        session.replace(customers).objects(CUSTOMERS).using(CUSTOMER_MAPPER).execute();
        session.replace(products).objects(PRODUCTS).using(PRODUCT_MAPPER).execute();
        session.replace(orders).objects(ORDERS).using(ORDER_MAPPER).execute();
        long totalTime = (System.nanoTime() - now) / 1_000;

        System.out.printf("Seed data loaded (%,d records) in %,dus.\n", CUSTOMERS.size() + PRODUCTS.size() + ORDERS.size(), totalTime);
    }
}
