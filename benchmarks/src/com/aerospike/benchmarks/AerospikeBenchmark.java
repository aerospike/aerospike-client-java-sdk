package com.aerospike.benchmarks;

import com.aerospike.client.fluent.Log;
import picocli.AutoComplete;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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

    @Override
    public Integer call() throws Exception {
        this.connectionOptions = Optional.ofNullable(connectionOptions).orElse(new ConnectionOptions());
        this.workloadOptions = Optional.ofNullable(workloadOptions).orElse(new WorkloadOptions());
        this.benchmarkOptions = Optional.ofNullable(benchmarkOptions).orElse(new BenchmarkOptions());
        BenchmarkContext benchmarkContext = BenchmarkContext.buildContext(connectionOptions, workloadOptions, benchmarkOptions);
        trackLatencyIfEnabled(benchmarkContext);
        runBenchmark(benchmarkContext);
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
        }

    }

    private void doInserts(BenchmarkContext benchmarkContext) throws InterruptedException {
        Arguments arguments = benchmarkContext.getArguments();
        ExecutorService es = getExecutorService(arguments);
        int numberOfThreads = arguments.getThreads();
        long numberOfKeys = arguments.getNumKeys();
        long tasks = numberOfThreads < numberOfKeys ? numberOfThreads : numberOfKeys;

        long keysPerTask = numberOfKeys / tasks;
        long rem = numberOfKeys - (keysPerTask * tasks);
        long start = arguments.getStartKey();

        for (long i = 0; i < tasks; i++) {
            long keyCount = (i < rem) ? keysPerTask  + 1 : keysPerTask;
            InsertTaskSync insertTask = new InsertTaskSync(benchmarkContext.getSession(), arguments, counters, start, keyCount);
            es.execute(insertTask);
            start += keyCount;
        }
        Thread.sleep(900);
       // collectInsertStats();
        es.shutdownNow();
    }

    private ExecutorService getExecutorService(Arguments arguments) {
        return Executors.newFixedThreadPool(arguments.getThreads());
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
