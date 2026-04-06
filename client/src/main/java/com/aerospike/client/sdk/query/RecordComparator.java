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

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.aerospike.client.sdk.AerospikeComparator;
import com.aerospike.client.sdk.RecordResult;

public class RecordComparator implements Comparator<RecordResult>{
    private final List<SortProperties> sortPropertiesList;
    private static final AerospikeComparator aerospikeComparatorCaseSensitive = new AerospikeComparator(true);
    private static final AerospikeComparator aerospikeComparatorCaseInsensitive = new AerospikeComparator(false);;

    public RecordComparator(List<SortProperties> sortProperties) {
        this.sortPropertiesList = sortProperties;
    }

    private AerospikeComparator getComparator(boolean caseSensitive) {
        if (caseSensitive) {
            return aerospikeComparatorCaseSensitive;
        }
        else {
            return aerospikeComparatorCaseInsensitive;
        }
    }

    private int compare(int index, Map<String, Object> map1, Map<String, Object> map2) {
        if (index >= sortPropertiesList.size()) {
            // Arbitrary sort
            return -1;
        }
        SortProperties sortProperties = sortPropertiesList.get(index);

        Object o1 = map1.get(sortProperties.name());
        Object o2 = map2.get(sortProperties.name());

        if (o1 == null && o2 == null) {
            return compare(index+1, map1, map2);
        }
        else if (o1 == null) {
            return sortProperties.sortDir() == SortDir.SORT_ASC ? -1 : 1;
        }
        else if (o2 == null) {
            return sortProperties.sortDir() == SortDir.SORT_ASC ? 1 : -1;
        }
        else {
            int result = this.getComparator(!sortProperties.caseInsensitive()).compare(o1, o2);
            if (result == 0) {
                // Identical elements, move onto the next one
                return compare(index+1, map1, map2);
            }
            else if (sortProperties.sortDir() == SortDir.SORT_DESC) {
                return -result;
            }
            else {
                return result;
            }
        }
    }

    private Map<String, Object> getBins(RecordResult kr) {
        if (kr == null || kr.recordOrNull() == null) {
            return null;
        }
        return kr.recordOrNull().bins;
    }

    @Override
    public int compare(RecordResult o1, RecordResult o2) {
        return compare(0, getBins(o1), getBins(o2));
    }

}
