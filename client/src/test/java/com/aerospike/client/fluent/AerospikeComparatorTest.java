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
package com.aerospike.client.fluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class AerospikeComparatorTest {

	private final AerospikeComparator cmp = new AerospikeComparator();
	private final AerospikeComparator cmpCaseInsensitive = new AerospikeComparator(false);

	// ========== Null ==========

	@Test
	public void nullEqualsNull() {
		assertEquals(0, cmp.compare(null, null));
	}

	@Test
	public void nullLessThanAnything() {
		assertTrue(cmp.compare(null, false) < 0);
		assertTrue(cmp.compare(null, 0) < 0);
		assertTrue(cmp.compare(null, "") < 0);
	}

	// ========== Boolean ==========

	@Test
	public void booleanOrdering() {
		assertEquals(0, cmp.compare(true, true));
		assertEquals(0, cmp.compare(false, false));
		assertTrue(cmp.compare(false, true) < 0);
		assertTrue(cmp.compare(true, false) > 0);
	}

	// ========== Integer ==========

	@Test
	public void integerOrdering() {
		assertEquals(0, cmp.compare(42, 42));
		assertTrue(cmp.compare(1, 2) < 0);
		assertTrue(cmp.compare(100, 50) > 0);
	}

	@Test
	public void integerSubtypesCompareByValue() {
		assertEquals(0, cmp.compare((byte) 5, 5L));
		assertEquals(0, cmp.compare((short) 100, 100));
		assertTrue(cmp.compare((byte) 1, 2L) < 0);
	}

	@Test
	public void negativeIntegers() {
		assertTrue(cmp.compare(-10, 10) < 0);
		assertTrue(cmp.compare(-1, -2) > 0);
		assertEquals(0, cmp.compare(-5L, -5));
	}

	// ========== Double ==========

	@Test
	public void doubleOrdering() {
		assertEquals(0, cmp.compare(3.14, 3.14));
		assertTrue(cmp.compare(1.0, 2.0) < 0);
		assertTrue(cmp.compare(9.9, 1.1) > 0);
	}

	@Test
	public void floatAndDoubleCompareByValue() {
		assertEquals(0, cmp.compare(1.0f, 1.0));
		assertTrue(cmp.compare(1.5f, 2.5) < 0);
	}

	// ========== String ==========

	@Test
	public void stringOrdering() {
		assertEquals(0, cmp.compare("abc", "abc"));
		assertTrue(cmp.compare("aa", "b") < 0);
		assertTrue(cmp.compare("z", "a") > 0);
	}

	@Test
	public void stringCaseSensitive() {
		assertTrue(cmp.compare("A", "a") < 0);
		assertTrue(cmp.compare("a", "A") > 0);
	}

	@Test
	public void stringCaseInsensitive() {
		assertEquals(0, cmpCaseInsensitive.compare("Alice", "alice"));
		assertEquals(0, cmpCaseInsensitive.compare("ABC", "abc"));
	}

	// ========== Byte array ==========

	@Test
	public void byteArrayOrdering() {
		byte[] a = {1, 2, 3};
		byte[] b = {1, 2, 4};
		byte[] c = {1, 2, 3};
		assertEquals(0, cmp.compare(a, c));
		assertTrue(cmp.compare(a, b) < 0);
		assertTrue(cmp.compare(b, a) > 0);
	}

	@Test
	public void byteArrayLengthDifference() {
		byte[] shorter = {1, 2};
		byte[] longer = {1, 2, 3};
		assertTrue(cmp.compare(shorter, longer) < 0);
		assertTrue(cmp.compare(longer, shorter) > 0);
	}

	// ========== List ==========

	@Test
	public void listElementwiseComparison() {
		List<Object> l1 = Arrays.asList(1, 2);
		List<Object> l2 = Arrays.asList(1, 3);
		assertTrue(cmp.compare(l1, l2) < 0);
	}

	@Test
	public void listEqualElements() {
		List<Object> l1 = Arrays.asList(1, 2, 3);
		List<Object> l2 = Arrays.asList(1, 2, 3);
		assertEquals(0, cmp.compare(l1, l2));
	}

	@Test
	public void listShorterIsLess() {
		List<Object> shorter = Arrays.asList(1, 2);
		List<Object> longer = Arrays.asList(1, 2, 1);
		assertTrue(cmp.compare(shorter, longer) < 0);
		assertTrue(cmp.compare(longer, shorter) > 0);
	}

	@Test
	public void listEmptyLessThanNonEmpty() {
		List<Object> empty = Arrays.asList();
		List<Object> nonEmpty = Arrays.asList(1);
		assertTrue(cmp.compare(empty, nonEmpty) < 0);
		assertTrue(cmp.compare(nonEmpty, empty) > 0);
	}

	@Test
	public void listBothEmpty() {
		assertEquals(0, cmp.compare(Arrays.asList(), Arrays.asList()));
	}

	// ========== Map ==========

	@Test
	public void mapSmallerSizeIsLess() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("a", 1);
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("a", 1);
		m2.put("b", 2);
		assertTrue(cmp.compare(m1, m2) < 0);
		assertTrue(cmp.compare(m2, m1) > 0);
	}

	@Test
	public void mapSameSizeDifferentKeys() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("a", 1);
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("b", 1);
		assertTrue(cmp.compare(m1, m2) < 0);
	}

	@Test
	public void mapSameSizeSameKeysDifferentValues() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("a", 1);
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("a", 2);
		assertTrue(cmp.compare(m1, m2) < 0);
	}

	@Test
	public void mapEqual() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("x", 10);
		m1.put("y", 20);
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("x", 10);
		m2.put("y", 20);
		assertEquals(0, cmp.compare(m1, m2));
	}

	// ========== GeoJSON ==========

	@Test
	public void geoJsonComparison() {
		Value.GeoJSONValue g1 = new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}");
		Value.GeoJSONValue g2 = new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}");
		Value.GeoJSONValue g3 = new Value.GeoJSONValue("{\"type\":\"Polygon\",\"coordinates\":[]}");
		assertEquals(0, cmp.compare(g1, g2));
		assertTrue(cmp.compare(g1, g3) != 0);
	}

	@Test
	public void geoJsonTypeMapping() {
		Value.GeoJSONValue g = new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[0,0]}");
		assertEquals(AerospikeComparator.AsType.GEOJSON, cmp.getType(g));
	}

	// ========== HLL ==========

	@Test
	public void hllTreatedAsBytes() {
		Value.HLLValue hll = new Value.HLLValue(new byte[]{1, 2, 3});
		assertEquals(AerospikeComparator.AsType.BYTES, cmp.getType(hll));
	}

	@Test
	public void hllComparesByByteContent() {
		Value.HLLValue h1 = new Value.HLLValue(new byte[]{1, 2, 3});
		Value.HLLValue h2 = new Value.HLLValue(new byte[]{1, 2, 3});
		Value.HLLValue h3 = new Value.HLLValue(new byte[]{1, 2, 4});
		assertEquals(0, cmp.compare(h1, h2));
		assertTrue(cmp.compare(h1, h3) < 0);
	}

	@Test
	public void hllComparesWithByteArray() {
		Value.HLLValue hll = new Value.HLLValue(new byte[]{1, 2, 3});
		byte[] raw = {1, 2, 3};
		assertEquals(0, cmp.compare(hll, raw));
		assertEquals(0, cmp.compare(raw, hll));
	}

	// ========== Cross-type ordering ==========

	@Test
	public void crossTypeOrderingMatchesAerospikeHierarchy() {
		// NIL(1) < BOOLEAN(2) < INTEGER(3) < STRING(4) < LIST(5) < MAP(6)
		// < BYTES(7) < DOUBLE(8) < GEOJSON(9)
		Object[] ordered = {
			null,                                          // NIL
			false,                                         // BOOLEAN
			42,                                            // INTEGER
			"hello",                                       // STRING
			Arrays.asList(1),                              // LIST
			Map.of("k", "v"),                              // MAP
			new byte[]{1},                                 // BYTES
			3.14,                                          // DOUBLE
			new Value.GeoJSONValue("{\"type\":\"Point\",\"coordinates\":[0,0]}") // GEOJSON
		};

		for (int i = 0; i < ordered.length; i++) {
			for (int j = i + 1; j < ordered.length; j++) {
				assertTrue(cmp.compare(ordered[i], ordered[j]) < 0,
					"Expected " + ordered[i] + " < " + ordered[j]);
				assertTrue(cmp.compare(ordered[j], ordered[i]) > 0,
					"Expected " + ordered[j] + " > " + ordered[i]);
			}
		}
	}

	@Test
	public void integerAlwaysLessThanDouble() {
		assertTrue(cmp.compare(999999, 0.001) < 0);
		assertTrue(cmp.compare(0.001, 999999) > 0);
	}

	// ========== OTHER type throws ==========

	@Test
	public void otherTypeThrows() {
		Date d1 = new Date();
		Date d2 = new Date();
		assertThrows(UnsupportedOperationException.class, () -> cmp.compare(d1, d2));
	}

	@Test
	public void otherTypeMapping() {
		assertEquals(AerospikeComparator.AsType.OTHER, cmp.getType(new Date()));
	}

	// ========== Nested structures ==========

	@Test
	public void listContainingMaps() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("a", 1);
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("a", 2);
		List<Object> l1 = new ArrayList<>(Arrays.asList(m1));
		List<Object> l2 = new ArrayList<>(Arrays.asList(m2));
		assertTrue(cmp.compare(l1, l2) < 0);
	}

	@Test
	public void mapContainingLists() {
		Map<Object, Object> m1 = new HashMap<>();
		m1.put("k", Arrays.asList(1, 2));
		Map<Object, Object> m2 = new HashMap<>();
		m2.put("k", Arrays.asList(1, 3));
		assertTrue(cmp.compare(m1, m2) < 0);
	}

	@Test
	public void deeplyNestedStructure() {
		List<Object> inner1 = Arrays.asList(1, 2);
		List<Object> inner2 = Arrays.asList(1, 3);
		Map<Object, Object> map1 = new HashMap<>();
		map1.put("data", inner1);
		Map<Object, Object> map2 = new HashMap<>();
		map2.put("data", inner2);
		List<Object> outer1 = new ArrayList<>(Arrays.asList(map1));
		List<Object> outer2 = new ArrayList<>(Arrays.asList(map2));
		assertTrue(cmp.compare(outer1, outer2) < 0);
	}
}
