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
package com.aerospike.client.sdk.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.AerospikeComparator;

public class AelPlaceholderBinderTest {

    @Test
    void bindSubstitutesInOrder() {
        PreparedAel p = PreparedAel.prepare("$.name == ?0 and $.x > ?1");
        assertThat(p.formValue("ael", 5)).isEqualTo("$.name == 'ael' and $.x > 5");
    }

    @Test
    void bindSubstitutesOutOfOrder() {
        assertThat(AelPlaceholderBinder.bind("?1 > ?0", 5, 10)).isEqualTo("10 > 5");
    }

    @Test
    void missingPlaceholderThrows() {
        assertThatThrownBy(() -> PreparedAel.prepare("$.x > ?0").formValue())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for placeholder ?0");
    }

    @Test
    void nullParamThrows() {
        assertThatThrownBy(() -> PreparedAel.prepare("$.x > ?0").formValue((Object) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing value for placeholder ?0");
    }

    @Test
    void bareQuestionMarkIsNotSubstituted() {
        assertThat(AelPlaceholderBinder.bind("$.x > ? and $.y > ?0", 5))
                .isEqualTo("$.x > ? and $.y > 5");
    }

    @Test
    void extraParamsAreIgnored() {
        assertThat(PreparedAel.prepare("$.x > ?0").formValue(5, 99)).isEqualTo("$.x > 5");
    }

    @Test
    void formatStringSingleQuotes() {
        assertThat(AelPlaceholderBinder.formatLiteral("ael")).isEqualTo("'ael'");
    }

    @Test
    void formatStringDoubleQuotesWhenSingleQuotePresent() {
        assertThat(AelPlaceholderBinder.formatLiteral("it's")).isEqualTo("\"it's\"");
    }

    @Test
    void formatBooleanLowerCase() {
        assertThat(AelPlaceholderBinder.formatLiteral(true)).isEqualTo("true");
        assertThat(AelPlaceholderBinder.formatLiteral(false)).isEqualTo("false");
    }

    @Test
    void formatIntegerAndLong() {
        assertThat(AelPlaceholderBinder.formatLiteral(42)).isEqualTo("42");
        assertThat(AelPlaceholderBinder.formatLiteral(42L)).isEqualTo("42");
    }

    @Test
    void formatFloatWithoutExponent() {
        String bound = PreparedAel.prepare("$.x > ?0").formValue(1e10);
        assertThat(bound).isEqualTo("$.x > 10000000000.0");
        assertThat(bound.toLowerCase()).doesNotContain("e");
    }

    @Test
    void formatSmallFloatWithoutExponent() {
        String bound = PreparedAel.prepare("$.x > ?0").formValue(0.0000001);
        assertThat(bound.toLowerCase()).doesNotContain("e");
        assertThat(bound).contains(".");
    }

    @Test
    void formatDoubleAsPlainDecimal() {
        assertThat(AelPlaceholderBinder.formatLiteral(3.14)).isEqualTo("3.14");
    }

    @Test
    void formatBlobAsHexLiteral() {
        byte[] data = new byte[] {1, 2, 3};
        assertThat(AelPlaceholderBinder.formatLiteral(data)).isEqualTo("X'010203'");
    }

    @Test
    void bindBlobInExpression() {
        PreparedAel blob = PreparedAel.prepare("$.b == ?0");
        assertThat(blob.formValue(new byte[] {(byte) 0xff, 0x00})).isEqualTo("$.b == X'ff00'");
    }

    @Test
    void formatList() {
        assertThat(AelPlaceholderBinder.formatLiteral(List.of(1, 2))).isEqualTo("[1, 2]");
    }

    @Test
    void formatMapWithSortedKeys() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("b", 2);
        map.put("a", 1);
        assertThat(AelPlaceholderBinder.formatLiteral(map)).isEqualTo("{'a': 1, 'b': 2}");
    }

    @Test
    void formatMapWithIntKey() {
        Map<Object, Object> map = new TreeMap<>(new AerospikeComparator());
        map.put(1, "one");
        assertThat(AelPlaceholderBinder.formatLiteral(map)).isEqualTo("{1: 'one'}");
    }

    @Test
    void prepareFactoryReturnsSameStatement() {
        PreparedAel p = PreparedAel.prepare("$.x > ?0");
        assertThat(p.getStatement()).isEqualTo("$.x > ?0");
    }

    @Test
    void nonFiniteFloatThrows() {
        assertThatThrownBy(() -> AelPlaceholderBinder.formatLiteral(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-finite");
    }
}
