package com.aerospike.benchmarks;

import com.aerospike.client.fluent.*;
import com.aerospike.client.fluent.util.RandomShift;

public abstract class RWTask {

    final Arguments args;
    final CounterStore counters;
    final long keyCount;
    final long keyStart;
    final String[] bNames;
    final String firstBin;
    boolean shouldStop;

    public RWTask(Arguments args, CounterStore counters) {
        this.args = args;
        this.counters = counters;
        this.keyCount = args.getNumKeys();
        this.keyStart = args.getStartKey();
        this.bNames = args.getBinNames(true);
        this.firstBin = bNames[0];
        this.shouldStop = false;
    }

    protected void runCommand(RandomShift random) {
        try {
            switch (args.getWorkload()) {
                case READ_UPDATE:
                    readUpdate(random);
                    break;
                case READ_REPLACE:
                    readReplace(random);
                    break;
                case READ_MODIFY_UPDATE:
                    readModifyUpdate(random);
                    break;
                case READ_MODIFY_INCREMENT:
                    readModifyIncrement(random);
                    break;
                case READ_MODIFY_DECREMENT:
                    readModifyDecrement(random);
                    break;
                default:
                    System.out.println("Not supported: " + args.getWorkload());
            }
        } catch (Exception e) {
            if (args.isDebug()) {
                e.printStackTrace();
            } else {
                System.out.println("Exception - " + e);
            }
        }
    }

    private void readModifyDecrement(RandomShift random) {
        long keyIdx = random.nextLong(keyCount);
        getBinsAndIncrement(new Key(args.getNamespace(), args.getSetName(), keyStart + keyIdx), -1);
    }

    private void readModifyIncrement(RandomShift random) {
        long keyIdx = random.nextLong(keyCount);
        getBinsAndIncrement(new Key(args.getNamespace(), args.getSetName(), keyStart + keyIdx) , 1);
    }

    private void readModifyUpdate(RandomShift random) {
        long key = random.nextLong(keyCount);
        // Read all bins.
        doRead(key, true);
        // Write one bin.
        doWrite(random, key, false);
    }

    private void readReplace(RandomShift random) {
        if (random.nextInt(100) < args.getReadPct()) {
            boolean isMultiBin = random.nextInt(100) < args.getReadMultiBinPct();

            if (args.getBatchSize() <= 1) {
                long key = random.nextLong(keyCount);
                doRead(key, isMultiBin);
            }
            else {
                // TODO read batch
            }
        } else {
            boolean isMultiBin = random.nextInt(100) < args.getWriteMultiBinPct();
            // Perform Single record write even if in batch mode.
            long key = random.nextLong(keyCount);
            doReplace(random, key, isMultiBin);
        }
    }

    private void readUpdate(RandomShift random) {
        if (random.nextInt(100) < args.getReadPct()) {
            boolean isMultiBin = random.nextInt(100) < args.getReadMultiBinPct();

            if (args.getBatchSize() <= 1) {
                long key = random.nextLong(keyCount);
                doRead(key, isMultiBin);
            }
            else {
                // TODO read batch
            }
        }
        else {
            boolean isMultiBin = random.nextInt(100) < args.getWriteMultiBinPct();
            // Perform Single record write even if in batch mode.
            long key = random.nextLong(keyCount);
            doWrite(random, key, isMultiBin);
        }
    }

    private void doReplace(RandomShift random, long keyIdx, boolean isMultiBin) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyStart + keyIdx);

        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, isMultiBin, keyStart + keyIdx);
        String[] nameArr = isMultiBin ? bNames  : new String[]{firstBin};
        try {
            createOrReplace(key, values, nameArr);
        }
        catch (AerospikeException ae) {
            writeFailure(ae);
        }
        catch (Exception e) {
            writeFailure(e);
        }
    }

    private void doWrite(RandomShift random, long keyIdx, boolean isMultiBin) {
        Key key = new Key(args.getNamespace(), args.getSetName(), keyStart + keyIdx);

        // Use predictable value for 0th bin same as key value
        Value[] values = args.getBinValues(random, isMultiBin, keyStart + keyIdx);
        String[] nameArr = isMultiBin ? bNames  : new String[]{firstBin};
        try {
            upsert(key, values, nameArr);
        }
        catch (AerospikeException ae) {
            writeFailure(ae);
        }
        catch (Exception e) {
            writeFailure(e);
        }
    }

    protected void writeFailure(AerospikeException ae) {
        if (ae.getResultCode() == ResultCode.TIMEOUT) {
            counters.write.timeouts.getAndIncrement();
        }
        else {
            counters.write.errors.getAndIncrement();
            if (args.isDebug()) {
                ae.printStackTrace();
            }
        }
    }

    protected void writeFailure(Exception e) {
        counters.write.errors.getAndIncrement();

        if (args.isDebug()) {
            e.printStackTrace();
        }
    }

    protected void doRead(long keyIdx, boolean isMultiBin) {
        try {
            Key key = new Key(args.getNamespace(), args.getSetName(), keyStart + keyIdx);
            // TODO UDF to be considered
            if (isMultiBin) {
                get(key);
            } else {
                get(key, "0");
            }
        } catch (AerospikeException ae) {
            readFailure(ae);
        } catch (Exception e) {
            readFailure(e);
        }
    }

    protected void processRead(Key key, RecordResult record) {
        if (record == null && args.isReportNotFound()) {
            counters.readNotFound.getAndIncrement();
        }
        else {
            counters.read.count.getAndIncrement();
        }
    }

    protected void readFailure(AerospikeException ae) {
        if (ae.getResultCode() == ResultCode.TIMEOUT) {
            counters.read.timeouts.getAndIncrement();
        }
        else {
            counters.read.errors.getAndIncrement();
            if (args.isDebug()) {
                ae.printStackTrace();
            }
        }
    }

    protected void readFailure(Exception e) {
        counters.read.errors.getAndIncrement();
        if (args.isDebug()) {
            e.printStackTrace();
        }
    }

    protected abstract void get(Key key, String binName);
    protected abstract void get(Key key);
    protected abstract void upsert(Key key, Value[] values, String... bins);
    protected abstract void createOrReplace(Key random, Value[] key, String... bins);
    protected abstract void getBinsAndIncrement(Key key, int incrementedBy);

    public void stop() {
        shouldStop = true;
    }
}
