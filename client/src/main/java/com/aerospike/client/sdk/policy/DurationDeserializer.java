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
package com.aerospike.client.sdk.policy;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

/**
 * SnakeYAML constructor for parsing Duration from human-readable formats like "10s", "20ms", "1m", etc.
 */
class DurationConstruct extends AbstractConstruct {

    private static final Pattern DURATION_PATTERN = Pattern.compile("^(\\d+)\\s*([a-zA-Z]+)$");

    @Override
    public Object construct(Node node) {
        String value = ((ScalarNode) node).getValue();
        return parseDuration(value);
    }

    /**
     * Parse a duration string - supports both human-readable and ISO-8601 formats
     */
    static Duration parseDuration(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        value = value.trim();

        // Try to parse as a human-readable duration first
        Duration duration = parseHumanReadableDuration(value);
        if (duration != null) {
            return duration;
        }

        // If that fails, try to parse as ISO-8601 duration
        try {
            return Duration.parse(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse duration: " + value +
                ". Expected format: <number><unit> (e.g., '10s', '20ms', '1m') or ISO-8601 duration", e);
        }
    }

    /**
     * Parse human-readable duration strings like "10s", "20ms", "1m", etc.
     */
    private static Duration parseHumanReadableDuration(String value) {
        Matcher matcher = DURATION_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }

        long amount = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2).toLowerCase();

        return switch (unit) {
            case "ns", "nanos", "nanosecond", "nanoseconds" -> Duration.ofNanos(amount);
            case "us", "micros", "microsecond", "microseconds" -> Duration.ofNanos(amount * 1000);
            case "ms", "millis", "millisecond", "milliseconds" -> Duration.ofMillis(amount);
            case "s", "sec", "second", "seconds" -> Duration.ofSeconds(amount);
            case "m", "min", "minute", "minutes" -> Duration.ofMinutes(amount);
            case "h", "hr", "hour", "hours" -> Duration.ofHours(amount);
            case "d", "day", "days" -> Duration.ofDays(amount);
            default -> null; // Unknown unit
        };
    }
}
