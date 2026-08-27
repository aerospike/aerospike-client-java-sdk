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
package com.aerospike.client.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.aerospike.client.sdk.query.SortDir;
import com.aerospike.client.sdk.query.SortProperties;
import com.aerospike.client.sdk.exp.Exp;

/**
 * Integration coverage for client-side {@link NavigatableRecordStream} sorting via
 * {@link com.aerospike.client.sdk.query.RecordComparator}.
 */
public class NavigatableRecordStreamSortTest extends ClusterTest {
    private static final String SET = "navsort";
    private static final String KEY_PREFIX = "navsort-";
    private static final String BIN_FRUIT = "fruit";
    private static final String BIN_GROUP = "grp";
    private static final String BIN_SEQ = "seq";
    private static final String BIN_SCORE = "score";
    private static final String BIN_RANK = "rank";
    private static final String BIN_TIE = "tie";
    private static final String TIE_KEY_PREFIX = "navsort-tie-";
    private static final int TIE_COUNT = 64;
    private static final String KEY_PASS = "navsort-pass";
    private static final String KEY_FAIL = "navsort-fail";
    private static final String KEY_MISSING = "navsort-missing";

    private static DataSet dataSet;
    private static DataSet edgeDataSet;
    private static DataSet tieDataSet;

    @BeforeAll
    public static void seedSortData() {
        dataSet = DataSet.of(args.namespace, SET);

        for (int i = 1; i <= 6; i++) {
            session.delete(dataSet.id(KEY_PREFIX + i)).execute();
        }

        // grp=1 ties on primary; seq breaks ties. Two records omit BIN_FRUIT.
        session.upsert(dataSet.id(KEY_PREFIX + 1))
            .bin(BIN_FRUIT).setTo("apple")
            .bin(BIN_GROUP).setTo(1)
            .bin(BIN_SEQ).setTo(20)
            .bin(BIN_SCORE).setTo(5)
            .bin(BIN_RANK).setTo(100)
            .execute();
        session.upsert(dataSet.id(KEY_PREFIX + 2))
            .bin(BIN_FRUIT).setTo("Banana")
            .bin(BIN_GROUP).setTo(1)
            .bin(BIN_SEQ).setTo(10)
            .bin(BIN_SCORE).setTo(5)
            .bin(BIN_RANK).setTo(200)
            .execute();
        session.upsert(dataSet.id(KEY_PREFIX + 3))
            .bin(BIN_FRUIT).setTo("cherry")
            .bin(BIN_GROUP).setTo(2)
            .bin(BIN_SEQ).setTo(30)
            .bin(BIN_SCORE).setTo(1)
            .bin(BIN_RANK).setTo(300)
            .execute();
        session.upsert(dataSet.id(KEY_PREFIX + 4))
            .bin(BIN_GROUP).setTo(1)
            .bin(BIN_SEQ).setTo(40)
            .bin(BIN_SCORE).setTo(3)
            .execute();
        session.upsert(dataSet.id(KEY_PREFIX + 5))
            .bin(BIN_GROUP).setTo(2)
            .bin(BIN_SEQ).setTo(50)
            .bin(BIN_SCORE).setTo(2)
            .execute();
        session.upsert(dataSet.id(KEY_PREFIX + 6))
            .bin(BIN_FRUIT).setTo("apple")
            .bin(BIN_GROUP).setTo(1)
            .bin(BIN_SEQ).setTo(30)
            .bin(BIN_SCORE).setTo(5)
            .bin(BIN_RANK).setTo(400)
            .execute();

        seedTieData();
        seedEdgeCaseKeys();
    }

    private static void seedTieData() {
        tieDataSet = DataSet.of(args.namespace, SET + "_tie");
        for (int i = 1; i <= TIE_COUNT; i++) {
            Key key = tieDataSet.id(TIE_KEY_PREFIX + i);
            session.delete(key).execute();
            session.upsert(key)
                .bin(BIN_TIE).setTo(42)
                .bin(BIN_SEQ).setTo(i)
                .execute();
        }
    }

    private static void seedEdgeCaseKeys() {
        edgeDataSet = DataSet.of(args.namespace, SET + "_edge");
        Key pass = edgeDataSet.id(KEY_PASS);
        Key fail = edgeDataSet.id(KEY_FAIL);
        Key missing = edgeDataSet.id(KEY_MISSING);

        session.delete(pass, fail, missing).execute();
        session.upsert(pass)
            .bin(BIN_SCORE).setTo(1)
            .execute();
        session.upsert(fail)
            .bin(BIN_SCORE).setTo(2)
            .execute();
    }

    @AfterAll
    public static void tearDownSortData() {
        for (int i = 1; i <= 6; i++) {
            session.delete(dataSet.id(KEY_PREFIX + i)).execute();
        }
        session.delete(
            edgeDataSet.id(KEY_PASS),
            edgeDataSet.id(KEY_FAIL),
            edgeDataSet.id(KEY_MISSING)
        ).execute();
        for (int i = 1; i <= TIE_COUNT; i++) {
            session.delete(tieDataSet.id(TIE_KEY_PREFIX + i)).execute();
        }
    }

    @Test
    public void multiFieldSortUsesSecondaryKeyOnPrimaryTies() {
        NavigatableRecordStream nav = loadAll().sortBy(List.of(
            SortProperties.ascending(BIN_GROUP),
            SortProperties.ascending(BIN_SEQ)));

        assertEquals(List.of(10, 20, 30, 40, 30, 50), collectSeq(nav));
    }

    @Test
    public void sortByDescendingSortProperties() {
        NavigatableRecordStream nav = loadAll().sortBy(SortProperties.descending(BIN_SEQ));

        assertEquals(List.of(50, 40, 30, 30, 20, 10), collectSeq(nav));
    }

    @Test
    public void sortByAscendingIgnoreCaseSortProperties() {
        NavigatableRecordStream nav = loadAll().sortBy(SortProperties.ascendingIgnoreCase(BIN_FRUIT));

        assertEquals(Arrays.asList(null, null, "apple", "apple", "Banana", "cherry"), collectFruit(nav));
    }

    @Test
    public void sortByDescendingIgnoreCaseSortProperties() {
        NavigatableRecordStream nav = loadAll().sortBy(SortProperties.descendingIgnoreCase(BIN_FRUIT));

        assertEquals(Arrays.asList("cherry", "Banana", "apple", "apple", null, null), collectFruit(nav));
    }

    @Test
    public void defaultSortByStringIsCaseInsensitive() {
        NavigatableRecordStream nav = loadAll().sortBy(BIN_FRUIT);

        assertEquals(Arrays.asList(null, null, "apple", "apple", "Banana", "cherry"), collectFruit(nav));
    }

    @Test
    public void sortByStringWithCaseInsensitiveFalseIsCaseSensitive() {
        // sortBy(String, boolean) names the flag caseInsensitive; the 3-arg overload
        // mislabels the same slot as caseSensitive but stores it in caseInsensitive.
        NavigatableRecordStream nav = loadAll().sortBy(BIN_FRUIT, false);

        assertEquals(Arrays.asList(null, null, "Banana", "apple", "apple", "cherry"), collectFruit(nav));
    }

    @Test
    public void sortBySortDirDescendingNegatesOrder() {
        NavigatableRecordStream nav = loadAll().sortBy(BIN_SCORE, SortDir.SORT_DESC);

        assertEquals(List.of(5, 5, 5, 3, 2, 1), collectScore(nav));
    }

    @Test
    public void missingSortBinSortsFirstWhenAscending() {
        NavigatableRecordStream nav = loadAll().sortBy(BIN_RANK);

        assertEquals(Arrays.asList(null, null, 100, 200, 300, 400), collectRank(nav));
    }

    @Test
    public void missingSortBinSortsLastWhenDescending() {
        NavigatableRecordStream nav = loadAll().sortBy(BIN_RANK, SortDir.SORT_DESC);

        assertEquals(Arrays.asList(400, 300, 200, 100, null, null), collectRank(nav));
    }

    /**
     * RecordResults with no record (filtered-out / missing-key) have null bins; sort must not NPE.
     */
    @Test
    public void sortBySurvivesFilteredOutResult() {
        List<Key> keys = List.of(edgeDataSet.id(KEY_PASS), edgeDataSet.id(KEY_FAIL));

        NavigatableRecordStream nav = session.query(keys)
            .where(Exp.eq(Exp.intBin(BIN_SCORE), Exp.val(1)))
            .failOnFilteredOut()
            .execute()
            .asNavigatableStream()
            .sortBy(BIN_SCORE);

        assertEquals(2, countResults(nav));
    }

    @Test
    public void sortBySurvivesMissingKeyResult() {
        List<Key> keys = List.of(edgeDataSet.id(KEY_PASS), edgeDataSet.id(KEY_MISSING));

        NavigatableRecordStream nav = session.query(keys)
            .includeMissingKeys()
            .execute()
            .asNavigatableStream()
            .sortBy(BIN_SCORE);

        assertEquals(Arrays.asList(null, 1), collectScoreAllowNull(nav));
    }

    @Test
    public void sortByIdenticalValuesCompletesForLargeTieGroup() {
        NavigatableRecordStream nav = session.query(tieDataSet)
            .execute()
            .asNavigatableStream()
            .sortBy(BIN_TIE);

        int count = 0;
        while (nav.hasNext()) {
            nav.next();
            count++;
        }
        assertEquals(TIE_COUNT, count);
    }

    private static NavigatableRecordStream loadAll() {
        List<Key> keys = new ArrayList<>(6);
        for (int i = 1; i <= 6; i++) {
            keys.add(dataSet.id(KEY_PREFIX + i));
        }
        return session.query(keys)
            .execute()
            .asNavigatableStream();
    }

    private static List<String> collectFruit(NavigatableRecordStream nav) {
        List<String> fruit = new ArrayList<>();
        while (nav.hasNext()) {
            fruit.add((String) nav.next().recordOrThrow().getValue(BIN_FRUIT));
        }
        return fruit;
    }

    private static List<Integer> collectSeq(NavigatableRecordStream nav) {
        List<Integer> seq = new ArrayList<>();
        while (nav.hasNext()) {
            seq.add(nav.next().recordOrThrow().getInt(BIN_SEQ));
        }
        return seq;
    }

    private static List<Integer> collectScore(NavigatableRecordStream nav) {
        List<Integer> scores = new ArrayList<>();
        while (nav.hasNext()) {
            scores.add(nav.next().recordOrThrow().getInt(BIN_SCORE));
        }
        return scores;
    }

    private static List<Integer> collectScoreAllowNull(NavigatableRecordStream nav) {
        List<Integer> scores = new ArrayList<>();
        while (nav.hasNext()) {
            RecordResult rr = nav.next();
            if (rr.getRecord() == null) {
                scores.add(null);
            }
            else {
                Object value = rr.getRecord().getValue(BIN_SCORE);
                scores.add(value == null ? null : ((Number) value).intValue());
            }
        }
        return scores;
    }

    private static int countResults(NavigatableRecordStream nav) {
        int count = 0;
        while (nav.hasNext()) {
            nav.next();
            count++;
        }
        return count;
    }

    private static List<Integer> collectRank(NavigatableRecordStream nav) {
        List<Integer> ranks = new ArrayList<>();
        while (nav.hasNext()) {
            Object value = nav.next().recordOrThrow().getValue(BIN_RANK);
            ranks.add(value == null ? null : ((Number) value).intValue());
        }
        return ranks;
    }
}
