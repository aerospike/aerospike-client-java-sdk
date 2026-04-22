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
package com.aerospike.benchmarks;

import java.util.Iterator;

import com.aerospike.client.sdk.util.RandomShift;

public class TransactionalWorkload implements Iterable<TransactionalItem> {

    private enum VariationType {
        PLUS,
        MINUS
    }

    // These options are derived and should not be set
    private int minReads;
    private int maxReads;
    private int minWrites;
    private int maxWrites;

    private static final TransactionalItem MULTI_BIN_WRITE = new TransactionalItem(TransactionalType.MULTI_BIN_WRITE);
    private static final TransactionalItem MULTI_BIN_READ = new TransactionalItem(TransactionalType.MULTI_BIN_READ);
    private static final TransactionalItem SINGLE_BIN_UPDATE = new TransactionalItem(TransactionalType.SINGLE_BIN_UPDATE);
    private static final TransactionalItem MULTI_BIN_UPDATE = new TransactionalItem(TransactionalType.MULTI_BIN_UPDATE);

    public TransactionalWorkload(String[] formatStrings) throws Exception {
        if (formatStrings == null || formatStrings.length == 0) {
            throw new Exception("invalid empty transactional workload string");
        }
        parseFormatStrings(formatStrings);
    }

    private void parseFormatStrings(String[] formatStrings) throws Exception {
        int reads = 0;
        int writes = 0;
        String variance = "";

        for (int i = 1; i < formatStrings.length; i++) {
            String thisOption = formatStrings[i];
            if (thisOption.length() < 3 || thisOption.charAt(1) != ':') {
                throw new Exception("Invalid transaction workload argument: " + thisOption);
            }
            String thisOptionValue = thisOption.substring(2);
            switch(thisOption.charAt(0)) {
                case 'r':
                    reads = Integer.parseInt(thisOptionValue);
                    break;
                case 'w':
                    writes = Integer.parseInt(thisOptionValue);
                    break;
                case 'v':
                    variance = thisOptionValue;
                    break;
                case 't':
                    throw new UnsupportedOperationException("Not supported yet: " + thisOption);
            }
        }

        if (reads < 0 || writes < 0) {
            throw new Exception("reads cannot be negative for transactional workload");
        } else if (reads == writes && reads == 0) {
            throw new Exception("no reads or writes defined for transactional workload");
        }

        this.minReads = applyVariance(reads, variance, VariationType.MINUS);
        this.maxReads = applyVariance(reads, variance, VariationType.PLUS);
        this.minWrites = applyVariance(writes, variance, VariationType.MINUS);
        this.maxWrites = applyVariance(writes, variance, VariationType.PLUS);
    }

    private int applyVariance(int base, String varianceStr, VariationType type) throws Exception{
        if (varianceStr == null || varianceStr.isEmpty() || base == 0) {
            return base;
        }

        // Parse the variance
        double variance;
        if (varianceStr.matches("^\\d+(\\.\\d+)?%$")) {
            // Percentage variance, like 23.4%
            double variancePct = Double.parseDouble(varianceStr.substring(0, varianceStr.length() - 1));
            variance = base * variancePct/100;
        }
        else if (varianceStr.matches("^\\d+$")) {
            // Absolute variance
            variance = Double.parseDouble(varianceStr);
        }
        else {
            throw new Exception("Cannot parse variance string '" + varianceStr + "'");
        }
        double result;
        if (type == VariationType.PLUS) {
            result = base + Math.floor(variance);
        }
        else {
            result = base - Math.floor(variance);
        }
        return Math.max(0, (int)result);
    }


    private class WorkloadIterator implements Iterator<TransactionalItem> {

        private int reads = 0;
        private int writes = 0;
        private final RandomShift random;

        public WorkloadIterator(RandomShift random) {
            this.random = random;
            if (this.random == null) {
                this.reads = (minReads + maxReads)/2;
                this.writes = (minWrites + maxWrites)/2;
            } else {
                reads = minReads + random.nextInt(maxReads - minReads + 1);
                writes = minWrites + random.nextInt(maxWrites - minWrites + 1);
            }
        }

        @Override
        public boolean hasNext() {
            return reads > 0 || writes > 0;
        }

        @Override
        public TransactionalItem next() {
            TransactionalItem result = null;
            // determine the result based on what's pending
            if (writes > 0 && reads > 0) {
                if (random == null) {
                    result = (writes > reads) ? MULTI_BIN_WRITE : MULTI_BIN_READ;
                } else {
                    int index = random.nextInt(writes + reads);
                    result = (index > writes) ? MULTI_BIN_READ : MULTI_BIN_WRITE;
                }
            } else if (reads > 0) {
                result = MULTI_BIN_READ;
            } else if (writes > 0) {
                result = MULTI_BIN_WRITE;
            }
            if (result.getType() != null && result.getType().isRead()) {
                reads--;
            } else {
                writes--;
            }
            // We never want to return back a WRITE, this really is either an update or a replace
            if (result.getType() == TransactionalType.MULTI_BIN_WRITE) {
                result = MULTI_BIN_UPDATE;
            } else if (result.getType() == TransactionalType.SINGLE_BIN_WRITE) {
                result = SINGLE_BIN_UPDATE;
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove is not supported");
        }
    }

    @Override
    public Iterator<TransactionalItem> iterator() {
        return new WorkloadIterator(null);
    }

    public Iterator<TransactionalItem> iterator(RandomShift random) {
        return new WorkloadIterator(random);
    }
}
