package com.aerospike.benchmarks;


import picocli.CommandLine.Option;

/**
 * HelpOptions provides command-line options for help and version information.
 *
 * <p>This class uses picocli annotations to define command-line flags for: - Displaying usage help
 * information - Showing version information
 *
 * <p>These options are typically included in command-line applications to provide standard help
 * functionality to users.
 */
public class HelpOptions {

    @Option(
            names = {"-u", "-usage", "--usage"},
            usageHelp = true,
            description = "prints usage options")
    private boolean usageHelpRequested;

    @Option(
            names = {"-V", "-version", "--version"},
            versionHelp = true,
            description = "Show version info")
    private boolean versionRequested;

}