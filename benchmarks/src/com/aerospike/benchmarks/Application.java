package com.aerospike.benchmarks;

import picocli.CommandLine;

import java.io.InputStream;
import java.util.Properties;

public class Application {

    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new AerospikeBenchmark());

        CommandLine gen = cmd.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);

        int exitCode = cmd.setColorScheme(colorScheme())
                .setParameterExceptionHandler(
                        (ex, args1) -> {
                            System.err.println("Parameter Error: " + ex.getMessage());
                            return 1;
                        })
                .setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
                    System.err.println("Execution Error: " + ex.getMessage());
                    return 1;
                })
                .execute(args);
        System.exit(exitCode);
    }


    /**
     * Creates and returns a customized color scheme for the command-line interface.
     *
     * <p>The color scheme defines how different elements of the command-line help output
     * are displayed:
     * - Commands are displayed as bold and underlined
     * - Options and parameters are displayed in yellow
     * - Option parameters are displayed in italic
     * - Error messages are displayed in bold red
     * - Stack traces are displayed in italic
     *
     * @return A configured {@link CommandLine.Help.ColorScheme} object with custom styling
     */
    public static CommandLine.Help.ColorScheme colorScheme() {
        return new CommandLine.Help.ColorScheme.Builder()
                .commands(CommandLine.Help.Ansi.Style.bold, CommandLine.Help.Ansi.Style.underline) // combine multiple styles
                .options(CommandLine.Help.Ansi.Style.fg_yellow) // yellow foreground color
                .parameters(CommandLine.Help.Ansi.Style.fg_yellow)
                .optionParams(CommandLine.Help.Ansi.Style.italic)
                .errors(CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
                .stackTraces(CommandLine.Help.Ansi.Style.italic)
                .build();
    }

    public static class VersionProvider implements CommandLine.IVersionProvider {

        @Override
        public String[] getVersion() throws Exception {
            Properties props = new Properties();
            try (InputStream in = Application.class.getResourceAsStream("project.properties")) {
                props.load(in);
            }
            return new String[] { props.getProperty("version", "developer-build") };
        }
    }
}