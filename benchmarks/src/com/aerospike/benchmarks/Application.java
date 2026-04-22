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
                            exceptionHandler(ex);
                            return 1;
                        })
                .setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
                    exceptionHandler(ex);
                    return 1;
                })
                .execute(args);
        System.exit(exitCode);
    }

    private static void exceptionHandler(Exception ex) {
        String message = ex.getMessage();
        Throwable cause = ex.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().isEmpty()) {
            message = (message != null && !message.isEmpty())
                    ? message + ": " + cause.getMessage()
                    : cause.getMessage();
        }
        if (message == null || message.isEmpty()) {
            message = ex.getClass().getSimpleName();
        }
        System.err.println("Execution Error: " + message);
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
            try (InputStream in = Application.class.getClassLoader().getResourceAsStream("project.properties")) {
                props.load(in);
            }
            return new String[] { props.getProperty("version", "developer-build") };
        }
    }
}