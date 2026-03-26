package com.aerospike.benchmarks;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.aerospike.client.sdk.ChainableOperationBuilder;
import com.aerospike.client.sdk.Value;
import com.aerospike.client.sdk.util.RandomShift;

import static com.aerospike.benchmarks.Constants.DEFAULT_NAMESPACE;
import static com.aerospike.benchmarks.Constants.DEFAULT_SET;

public class Arguments {

    private String namespace;
    private String[] batchNamespaces;
    private String setName;
    private Workload workload;
    private String binNameBase;
    private int batchSize;
    private int nBins;
    private int readPct;
    private int readMultiBinPct;
    private int writeMultiBinPct;
    private int throughput;
    private Set<Integer> partitionIds;
    private long transactionLimit;
    private boolean reportNotFound;
    private boolean debug;
    private int nThreads;
    private long numKeys;
    private long startKey;
    private int asyncMaxCommands;
    private TransactionalWorkload transactionalWorkload;
    public DBObjectSpec[] objectSpec;

    private boolean hasTransactions;

    private Long durationSeconds;


    public boolean isInitialisation() {
        return Objects.equals(workload, Workload.INITIALIZE);
    }

    public static Arguments toArgs(WorkloadOptions workloadOpts, BenchmarkOptions benchmarkOpts) throws Exception {
        Arguments args = new Arguments();
        if (workloadOpts.getBatchNamespaces() != null) {
            args.batchNamespaces = workloadOpts.getBatchNamespaces().split(",");
        }
        args.namespace = Optional.ofNullable(workloadOpts.getNamespace()).orElse(DEFAULT_NAMESPACE);
        args.setName = Optional.ofNullable(workloadOpts.getSet()).orElse(DEFAULT_SET);
        args.binNameBase = workloadOpts.getBinNameBase();
        args.nBins = workloadOpts.getBins();
        args.debug = benchmarkOpts.isDebug();
        WorkloadContext workloadContext = WorkloadContext.toWorkloadContext(workloadOpts.getWorkload());
        args.workload = workloadContext.workload;
        args.readPct = workloadContext.readPct;
        args.readMultiBinPct = workloadContext.readMultiBinPct;
        args.writeMultiBinPct = workloadContext.writeMultiBinPct;
        args.nThreads = benchmarkOpts.getThreads() != null ? benchmarkOpts.getThreads() : benchmarkOpts.getVirtualThreads();
        args.numKeys = workloadOpts.getKeys();
        args.startKey = Optional.ofNullable(workloadOpts.getStartKey()).orElse(0L);
        args.objectSpec = toDbObjectSpecs(workloadOpts);
        args.throughput = Optional.ofNullable(workloadOpts.getThroughput()).orElse(0);
        args.transactionLimit = Optional.ofNullable(workloadOpts.getTransactions()).orElse(Long.MAX_VALUE);
        args.durationSeconds = workloadOpts.getDurationSeconds();
        args.asyncMaxCommands = Optional.ofNullable(benchmarkOpts.getAsyncMaxCommands()).orElse(100);
        args.batchSize = Optional.ofNullable(benchmarkOpts.getBatchSize()).orElse(1);
        args.transactionalWorkload = workloadContext.transactionalWorkload();
        if (workloadContext.transactionalWorkload() != null) {
            args.hasTransactions = true;
        }
        return args;
    }

    private static DBObjectSpec[] toDbObjectSpecs(WorkloadOptions workloadOpts) throws Exception {
        if (workloadOpts.getObjectSpec() == null) {
            return new DBObjectSpec[]{new DBObjectSpec()};
        }

        String[] objectsArr = workloadOpts.getObjectSpec().split(",");
        DBObjectSpec[] objectSpecs = new DBObjectSpec[objectsArr.length];
        for (int i = 0; i < objectsArr.length; i++) {
            try {
                DBObjectSpec spec = new DBObjectSpec(objectsArr[i]);
                objectSpecs[i] = spec;
            } catch (Throwable t) {
                throw new Exception("Invalid object spec: " + objectsArr[i] + "\n" + t.getMessage());
            }
        }
        return objectSpecs;

    }


    public String getNamespace() {
        return namespace;
    }

    public String[] getBatchNamespaces() {
        return batchNamespaces;
    }

    public String getSetName() {
        return setName;
    }

    public Workload getWorkload() {
        return workload;
    }

    public String getBinNameBase() {
        return binNameBase;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getnBins() {
        return nBins;
    }

    public int getReadPct() {
        return readPct;
    }

    public int getReadMultiBinPct() {
        return readMultiBinPct;
    }

    public int getWriteMultiBinPct() {
        return writeMultiBinPct;
    }

    public int getThroughput() {
        return throughput;
    }

    public Set<Integer> getPartitionIds() {
        return partitionIds;
    }

    public long getTransactionLimit() {
        return transactionLimit;
    }

    public Long getDurationSeconds() {
        return durationSeconds;
    }

    public boolean isReportNotFound() {
        return reportNotFound;
    }

    public boolean isDebug() {
        return debug;
    }

    public int getThreads() {
        return nThreads;
    }

    public long getNumKeys() {
        return numKeys;
    }

    public long getStartKey() {
        return startKey;
    }

    public int getAsyncMaxCommands() {
        return asyncMaxCommands;
    }

    public TransactionalWorkload getTransactionalWorkload() {
        return transactionalWorkload;
    }

    public boolean isHasTransactions() {
        return hasTransactions;
    }


    public String[] getBinNames(boolean multiBin) {
        int binCount = (multiBin) ? nBins : 1;
        String[] names = new String[binCount];
        for (int i = 0; i < binCount; i++) {
            names[i] = binNameBase + "_" + (i + 1);
        }
        return names;
    }

    /** Returns bin values for the current config (0th bin uses keySeed, others random). */
    public Value[] getBinValues(RandomShift random, boolean multiBin, long keySeed) {
        int binCount = (multiBin) ? nBins : 1;
        Value[] values = new Value[binCount];
        fillBinValues(values, random, binCount, keySeed);
        return values;
    }

    private void fillBinValues(Value[] out, RandomShift random, int binCount, long keySeed) {
        int specLength = objectSpec.length;
        for (int i = 0; i < binCount; i++) {
            out[i] = genValue(random, objectSpec[i % specLength], i == 0 ? keySeed : -1);
        }
    }

    public void setBinFromValue(ChainableOperationBuilder builder, String bName, Value value) {
        Object v = value.getObject();
        if (v == null) {
            builder.bin(bName).remove();
            return;
        }
        switch (v) {
            case String s -> {
                builder.bin(bName).setTo(s);
                return;
            }
            case Integer i -> {
                builder.bin(bName).setTo(i);
                return;
            }
            case Long l -> {
                builder.bin(bName).setTo(l);
                return;
            }
            case Double aDouble -> {
                builder.bin(bName).setTo(aDouble);
                return;
            }
            case Boolean b -> {
                builder.bin(bName).setTo(b);
                return;
            }
            case byte[] bytes -> {
                builder.bin(bName).setTo(bytes);
                return;
            }
            default -> {}
        }
        builder.bin(bName).setTo(String.valueOf(v));
    }

    private Value genValue(RandomShift random, DBObjectSpec spec, long keySeed) {
        switch (spec.type) {
            default:
            case INTEGER:
                return Value.get(keySeed == -1 ? random.nextInt() : keySeed);
            case BYTES:
                byte[] ba = new byte[spec.size];
                random.nextBytes(ba);
                return Value.get(ba);

            case STRING:
                StringBuilder sb = new StringBuilder(spec.size);
                for (int i = 0; i < spec.size; i++) {
                    // Append ascii value between ordinal 33 and 127.
                    sb.append((char)(random.nextInt(94) + 33));
                }
                return Value.get(sb.toString());

            case RANDOM:
                byte[] bytes = new byte[spec.size * 8];
                int idx = 0;
                int rand_pct = spec.randPct;
                if (rand_pct < 100) {
                    int n_zeros = (spec.size * (100 - rand_pct)) / 100;
                    int n_rands = spec.size - n_zeros;
                    for (int z = n_zeros; z != 0; z--) {
                        idx = longToBytes(0, bytes, idx);
                    }
                    for (int r = n_rands; r != 0; r--) {
                        long l = random.nextLong();
                        idx = longToBytes(l, bytes, idx);
                    }
                }
                while (idx < spec.size) {
                    long l = random.nextLong();
                    idx = longToBytes(l, bytes, idx);
                }
                return Value.get(bytes);

            case TIMESTAMP:
                return Value.get(System.currentTimeMillis());
        }
    }

    private static int longToBytes(long l, byte[] bytes, int offset) {
        for (int i = offset + 7; i >= offset; i--) {
            bytes[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return offset + 8;
    }


    private record WorkloadContext(Workload workload, int readPct, int readMultiBinPct, int writeMultiBinPct,
                                   TransactionalWorkload transactionalWorkload) {

        static WorkloadContext toWorkloadContext(String workloadStr) {
            if (workloadStr == null || workloadStr.isBlank()) {
                throw new IllegalArgumentException("Workload string must not be null or blank");
            }
            String[] parts = workloadStr.trim().split(",");
            String type = parts[0].trim().toUpperCase();

            return switch (type) {
                case "I" -> parseInitialize(parts);
                case "RU" -> parseReadUpdate(parts, Workload.READ_UPDATE);
                case "RR" -> parseReadUpdate(parts, Workload.READ_REPLACE);
                case "RMU" -> parseSingleArgWorkload(parts, Workload.READ_MODIFY_UPDATE);
                case "RMI" -> parseSingleArgWorkload(parts, Workload.READ_MODIFY_INCREMENT);
                case "RMD" -> parseSingleArgWorkload(parts, Workload.READ_MODIFY_DECREMENT);
                case "TXN" -> parseTransactional(parts);
                default -> throw new IllegalArgumentException("Unknown workload: " + type);
            };
        }

        private static WorkloadContext parseInitialize(String[] parts) {
            if (parts.length != 1) {
                throw new IllegalArgumentException(
                        "Invalid workload arguments for I: expected 1, got " + parts.length);
            }
            return new WorkloadContext(Workload.INITIALIZE, 0, 0, 0, null);
        }

        private static WorkloadContext parseReadUpdate(String[] parts, Workload workload) {
            if (parts.length < 2 || parts.length > 4) {
                throw new IllegalArgumentException(
                        "Invalid workload arguments for " + workload + ": expected 2 to 4, got " + parts.length);
            }
            int readPct = Integer.parseInt(parts[1].trim());
            if (readPct < 0 || readPct > 100) {
                throw new IllegalArgumentException(
                        "Read percentage must be between 0 and 100, got " + readPct);
            }
            int readMultiBinPct = parts.length >= 3 ? Integer.parseInt(parts[2].trim()) : 100;
            int writeMultiBinPct = parts.length >= 4 ? Integer.parseInt(parts[3].trim()) : 100;
            return new WorkloadContext(workload, readPct, readMultiBinPct, writeMultiBinPct, null);
        }

        private static WorkloadContext parseSingleArgWorkload(String[] parts, Workload workload) {
            if (parts.length != 1) {
                throw new IllegalArgumentException(
                        "Invalid workload arguments for " + workload + ": expected 1, got " + parts.length);
            }
            return new WorkloadContext(workload, 0, 0, 0, null);
        }

        private static WorkloadContext parseTransactional(String[] parts) {
            try {
                TransactionalWorkload txn = new TransactionalWorkload(parts);
                return new WorkloadContext(Workload.TRANSACTION, 0, 0, 0, txn);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid transactional workload", e);
            }
        }
    }

}
