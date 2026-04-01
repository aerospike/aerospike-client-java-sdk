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
package com.aerospike.client.fluent;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Aerospike client logging facility. Logs can be filtered and message callbacks
 * can be defined to control how log messages are written.
 */
public final class Log {
	/**
	 * Log escalation level.
	 */
	public enum Level {
		/**
		 * Error condition has occurred.
		 */
		ERROR,

		/**
		 * Unusual non-error condition has occurred.
		 */
		WARN,

		/**
		 * Normal information message.
		 */
		INFO,

		/**
		 * Message used for debugging purposes.
		 */
		DEBUG
	}

	/**
	 * Additional context sent to log callback messages.
	 */
	public static class Context {
		/**
		 * Empty context for use when context is not available.
		 */
		public static final Context Empty = new Context("");

		/**
		 * Cluster name. Will be empty string if clusterName is not defined in ClusterDefinition
		 * or the log message is not associated with a cluster.
		 */
		public final String clusterName;

		/**
		 * Creates a context with the given cluster name for {@link Callback#log(Context, Level, String)}.
		 *
		 * @param clusterName cluster label for the message, or empty string if none
		 */
		public Context(String clusterName)
		{
			this.clusterName = clusterName;
		}
	}

	/**
	 * An object implementing this interface may be passed in to
	 * {@link #setCallback(Callback callback) setCallback()},
	 * so the caller can channel Aerospike client logs as desired.
	 */
	public static interface Callback {
		/**
		 * This method will be called for each client log statement.
		 *
		 * @param context	additional context associated with the log message
		 * @param level		log level
		 * @param message	log message
		 */
		public void log(Context context, Level level, String message);
	}

	private static volatile Level gLevel = Level.INFO;
	private static volatile Callback gCallback = null;
	private static volatile boolean gCallbackSet = false;

	/**
	 * Set log level filter.
	 *
	 * @param level			only show logs at this or more urgent level
	 */
	public static void setLevel(Level level) {
		gLevel = level;
	}

	/**
	 * Set log callback. To silence the log, set callback to null.
	 *
	 * @param callback		{@link Callback} implementation
	 */
	public static void setCallback(Callback callback) {
		gCallback = callback;
		gCallbackSet = true;
	}

	/**
	 * Log messages to terminal standard output with timestamp, level and message.
	 */
	public static void setCallbackStandard() {
		setCallback(new Log.Standard());
	}

	/**
	 * Determine if the log callback default (null - disable logging) has been overridden by the user.
	 * Return true if the user explicitly defined a callback or disabled logging by setting the log callback to null.
	 * Return false if the log callback default was not overridden by the user.
	 */
	public static boolean isSet() {
		return gCallbackSet;
	}

	/**
	 * Determine if error log level is enabled.
	 */
	public static boolean errorEnabled() {
		return gCallback != null;
	}

	/**
	 * Determine if warning log level is enabled.
	 */
	public static boolean warnEnabled() {
		return gCallback != null && Level.WARN.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Determine if info log level is enabled.
	 */
	public static boolean infoEnabled() {
		return gCallback != null && Level.INFO.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Determine if debug log level is enabled.
	 */
	public static boolean debugEnabled() {
		return gCallback != null && Level.DEBUG.ordinal() <= gLevel.ordinal();
	}

	/**
	 * Log an error message.
	 */
	public static void error(String message) {
		log(Level.ERROR, message);
	}

	/**
	 * Log an error message with context.
	 */
	public static void error(Context context, String message) {
		log(context, Level.ERROR, message);
	}

	/**
	 * Log warning message.
	 */
	public static void warn(String message) {
		log(Level.WARN, message);
	}

	/**
	 * Log warning message with context.
	 */
	public static void warn(Context context, String message) {
		log(context, Level.WARN, message);
	}

	/**
	 * Log info message.
	 */
	public static void info(String message) {
		log(Level.INFO, message);
	}

	/**
	 * Log info message with context.
	 */
	public static void info(Context context, String message) {
		log(context, Level.INFO, message);
	}

	/**
	 * Log debug message.
	 */
	public static void debug(String message) {
		log(Level.DEBUG, message);
	}

	/**
	 * Log debug message with context.
	 */
	public static void debug(Context context, String message) {
		log(context, Level.DEBUG, message);
	}

	/**
	 * Filter and forward message to callback.
	 *
	 * @param level			message severity level
	 * @param message		message string not terminated with a newline
	 */
	public static void log(Level level, String message) {
		if (gCallback != null && level.ordinal() <= gLevel.ordinal()) {
			try {
				gCallback.log(Context.Empty, level, message);
			}
			catch (Throwable e) {
			}
		}
	}

	/**
	 * Filter and forward a message to the registered callback with cluster (or other) context.
	 *
	 * @param context additional context (e.g. {@link Context#clusterName}); use {@link Context#Empty} if none
	 * @param level   message severity level
	 * @param message message string not terminated with a newline
	 */
	public static void log(Context context, Level level, String message) {
		if (gCallback != null && level.ordinal() <= gLevel.ordinal()) {
			try {
				gCallback.log(context, level, message);
			}
			catch (Throwable e) {
			}
		}
	}

	private static class Standard implements Log.Callback {
		private static final DateTimeFormatter Formatter =
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

		@Override
		public void log(Context context, Level level, String message) {
			StringBuilder sb = new StringBuilder(message.length() + 128);

			sb.append(LocalDateTime.now().format(Formatter));

			if (context.clusterName != null && context.clusterName.length() > 0)
			{
				sb.append(' ');
				sb.append(context.clusterName);
			}

			sb.append(' ');
			sb.append(level.toString());
			sb.append(' ');
			sb.append(message);

			System.out.println(sb.toString());
		}
	}
}
