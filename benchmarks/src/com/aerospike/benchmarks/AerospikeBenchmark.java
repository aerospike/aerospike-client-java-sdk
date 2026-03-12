package com.aerospike.benchmarks;

import com.aerospike.client.fluent.Log;
import picocli.AutoComplete;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;


@Command(
        name = Constants.RUN_BENCHMARKS,
        mixinStandardHelpOptions = false,
        versionProvider = Application.VersionProvider.class,
        description = "%n" + Constants.USAGE_MESSAGE + "%n",
        descriptionHeading = "%nDescription:%n",
        footerHeading = "%n %n",
        footer = "For more information, visit https://www.aerospike.com. Copyright (c) 2025",
        subcommands = {AutoComplete.GenerateCompletion.class},
        usageHelpAutoWidth = true
)
public class AerospikeBenchmark implements Callable<Integer>, Log.Callback {

    private static final DateTimeFormatter TimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Spec
    public CommandSpec spec;

    @ArgGroup(exclusive = false, heading = "%n" + Constants.CONN_OPT_HEADING + "%n")
    public ConnectionOptions connectionOptions;

    @ArgGroup(exclusive = false, heading = "%n" + Constants.WORKLOAD_OPT_HEADING + "%n")
    public WorkloadOptions workloadOptions;

    @ArgGroup(exclusive = false, heading = "%n" + Constants.BENCHMARK_OPT_HEADING + "%n")
    public BenchmarkOptions benchmarkOptions;

    @ArgGroup(exclusive = false, heading = "%n" + Constants.HELP_OPT_HEADING + "%n")
    public HelpOptions helpOptions;

    private final CounterStore counters = new CounterStore();

    private Supplier<ExecutorService> executorSupplier;

    @Override
    public Integer call() throws Exception {
        this.connectionOptions = Optional.ofNullable(connectionOptions).orElse(new ConnectionOptions());
        this.workloadOptions = Optional.ofNullable(workloadOptions).orElse(new WorkloadOptions());
        this.benchmarkOptions = Optional.ofNullable(benchmarkOptions).orElse(new BenchmarkOptions());
        executorSupplier = () -> Executors.newFixedThreadPool(benchmarkOptions.getThreads());
        try (BenchmarkContext benchmarkContext = BenchmarkContext.buildContext(connectionOptions, workloadOptions, benchmarkOptions)) {
            trackLatencyIfEnabled(benchmarkContext);
            runBenchmark(benchmarkContext);
        }
        return 0;
    }

    private void trackLatencyIfEnabled(BenchmarkContext context) throws Exception {
        if (!context.isLatenciesEnabled()) {
            return;
        }
        counters.write.latency = context.getLatencyManager(Constants.OP_TYPE.w);
        counters.read.latency = context.getLatencyManager(Constants.OP_TYPE.r);
        if (context.isHasTxns()) {
            // TODO set transaction latency
        }
    }

    private void runBenchmark(BenchmarkContext benchmarkContext) throws Exception {
        final Arguments arguments = benchmarkContext.getArguments();
        boolean isInitialisation = arguments.isInitialisation();

        if (isInitialisation) {
            doInserts(benchmarkContext);
        } else {
            doRwTask(benchmarkContext);
        }

    }

    private void doRwTask(BenchmarkContext benchmarkContext) throws InterruptedException {
        int threads = benchmarkOptions.getThreads();
        ExecutorService es = executorSupplier.get();
        Arguments arguments = benchmarkContext.getArguments();
        RWTask[] tasks = new RWTask[threads];

        if (benchmarkOptions.isAsync()) {
            Semaphore inFlight = new Semaphore(arguments.getAsyncMaxCommands());
            for (int i = 0; i < threads; i++) {
                RWTaskAsync rt = new RWTaskAsync(arguments, counters, benchmarkContext.getSession(), inFlight);
                tasks[i] = rt;
                es.execute(rt);
            }
        } else {
            for (int i = 0; i < threads; i++) {
                RWTaskSync rt = new RWTaskSync(arguments, counters, benchmarkContext.getSession());
                tasks[i] = rt;
                es.execute(rt);
            }
        }
        Thread.sleep(900);
        collectRwStats(benchmarkContext.getArguments(), tasks);
        es.shutdown();
    }

    private void doInserts(BenchmarkContext benchmarkContext) throws InterruptedException {
        Arguments arguments = benchmarkContext.getArguments();
        ExecutorService es = executorSupplier.get();
        int numberOfThreads = arguments.getThreads();
        long numberOfKeys = arguments.getNumKeys();
        long tasks = numberOfThreads < numberOfKeys ? numberOfThreads : numberOfKeys;

        long keysPerTask = numberOfKeys / tasks;
        long rem = numberOfKeys - (keysPerTask * tasks);
        long start = arguments.getStartKey();

        if (benchmarkOptions.isAsync()) {
            Semaphore inFlight = new Semaphore(arguments.getAsyncMaxCommands());
            for (long i = 0; i < tasks; i++) {
                long keyCount = (i < rem) ? keysPerTask  + 1 : keysPerTask;
                InsertTaskAsync insertTask = new InsertTaskAsync(benchmarkContext.getSession(),
                        arguments,
                        counters,
                        inFlight,
                        start,
                        keyCount);
                es.execute(insertTask);
                start += keyCount;
            }
        } else {
            for (long i = 0; i < tasks; i++) {
                long keyCount = (i < rem) ? keysPerTask  + 1 : keysPerTask;
                InsertTaskSync insertTask = new InsertTaskSync(benchmarkContext.getSession(),
                        arguments,
                        counters,
                        start,
                        keyCount);
                es.execute(insertTask);
                start += keyCount;
            }
        }

        Thread.sleep(900);
        collectInsertStats(arguments);
        es.shutdownNow();
    }

    private void collectInsertStats(Arguments arguments) throws InterruptedException {
        int total = 0;

        while (total < arguments.getNumKeys()) {
            long time = System.currentTimeMillis();

            int numWrites = this.counters.write.count.getAndSet(0);
            int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
            int errorWrites = this.counters.write.errors.getAndSet(0);
            total += numWrites;

            this.counters.periodBegin.set(time);

            LocalDateTime dt =
                    Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
            System.out.println(
                    dt.format(TimeFormatter)
                            + " write(count="
                            + total
                            + " tps="
                            + numWrites
                            + " timeouts="
                            + timeoutWrites
                            + " errors="
                            + errorWrites
                            + ")");

            if (this.counters.write.latency != null) {
                this.counters.write.latency.printHeader(System.out);
                this.counters.write.latency.printResults(System.out, Constants.OP_TYPE.w.opType);
            }
            Thread.sleep(1000);
        }

        if (this.counters.write.latency != null) {
            this.counters.write.latency.printSummaryHeader(System.out);
            this.counters.write.latency.printSummary(System.out, Constants.OP_TYPE.w.opType);
        }
    }

    private void collectRwStats(Arguments arguments, RWTask[] tasks) throws InterruptedException {
        long transactionTotal = 0;

        while (true) {
            long time = System.currentTimeMillis();
            //int notFound = arguments.isReportNotFound() ? this.counters.readNotFound.getAndSet(0) : 0;
            this.counters.periodBegin.set(time);

            int numWrites = this.counters.write.count.getAndSet(0);
            int timeoutWrites = this.counters.write.timeouts.getAndSet(0);
            int errorWrites = this.counters.write.errors.getAndSet(0);
            LocalDateTime dt =
                    Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
            System.out.print(dt.format(TimeFormatter));
            System.out.print(
                    " write(tps="
                            + numWrites
                            + " timeouts="
                            + timeoutWrites
                            + " errors="
                            + errorWrites
                            + ")");

            int numReads = this.counters.read.count.getAndSet(0);
            int timeoutReads = this.counters.read.timeouts.getAndSet(0);
            int errorReads = this.counters.read.errors.getAndSet(0);
            System.out.print(
                    " read(tps=" + numReads + " timeouts=" + timeoutReads + " errors=" + errorReads + ")");

            // TODO txn calc

            System.out.print(
                    " total(tps="
                            + (numWrites + numReads)
                            + " timeouts="
                            + (timeoutWrites + timeoutReads)
                            + " errors="
                            + (errorWrites + errorReads)
                            + ")");
            System.out.println();

            if (this.counters.write.latency != null) {
                this.counters.write.latency.printHeader(System.out);
                this.counters.write.latency.printResults(System.out, Constants.OP_TYPE.w.opType);
                this.counters.read.latency.printResults(System.out, Constants.OP_TYPE.r.opType);
                if (this.counters.transaction != null && this.counters.transaction.latency != null) {
                    this.counters.transaction.latency.printResults(System.out, Constants.OP_TYPE.tx.opType);
                }
            }

            if (arguments.getTransactionLimit() > 0) {
                transactionTotal +=
                        numWrites + timeoutWrites + errorWrites + numReads + timeoutReads + errorReads;
                if (transactionTotal >= arguments.getTransactionLimit()) {
                    for (RWTask task : tasks) {
                        task.stop();
                    }
                    if (this.counters.write.latency != null) {
                        this.counters.write.latency.printSummaryHeader(System.out);
                        this.counters.write.latency.printSummary(System.out, Constants.OP_TYPE.w.opType);
                        this.counters.read.latency.printSummary(System.out, Constants.OP_TYPE.r.opType);
                        if (this.counters.transaction != null && this.counters.transaction.latency != null) {
                            this.counters.transaction.latency.printSummary(System.out, Constants.OP_TYPE.tx.opType);
                        }
                    }

                    System.out.println("Transaction limit reached: " + arguments.getTransactionLimit() + ". Exiting.");
                    break;
                }
            }
            Thread.sleep(1000);
        }
    }


    @Override
    public void log(Log.Context context, Log.Level level, String message) {
        System.out.println(
                LocalDateTime.now().format(TimeFormatter)
                        + ' '
                        + level.toString()
                        + ' '
                        + message);
    }

}
