package com.aerospike.client.fluent.policy;

import java.io.IOException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * Custom deserializer for Duration that supports human-readable formats like "10s", "20ms", "1m", etc.
 */
public class DurationDeserializer extends JsonDeserializer<Duration> {
    
    // Pattern to match duration strings like "10s", "20ms", "1m", "2h", etc.
    private static final Pattern DURATION_PATTERN = Pattern.compile(
        "^(\\d+)\\s*([a-zA-Z]+)$"
    );
    
    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String value = p.getValueAsString();
        
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        
        value = value.trim();
        
        // Try to parse as a human-readable duration first
        Duration duration = parseHumanReadableDuration(value);
        if (duration != null) {
            return duration;
        }
        
        // If that fails, try to parse as ISO-8601 duration (Jackson's default format)
        try {
            return Duration.parse(value);
        } catch (Exception e) {
            throw new IOException("Cannot parse duration: " + value + 
                ". Expected format: <number><unit> (e.g., '10s', '20ms', '1m') or ISO-8601 duration", e);
        }
    }
    
    /**
     * Parse human-readable duration strings like "10s", "20ms", "1m", etc.
     */
    private Duration parseHumanReadableDuration(String value) {
        Matcher matcher = DURATION_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }
        
        long amount = Long.parseLong(matcher.group(1));
        String unit = matcher.group(2).toLowerCase();
        
        switch (unit) {
            case "ns":
            case "nanos":
            case "nanosecond":
            case "nanoseconds":
                return Duration.ofNanos(amount);
                
            case "us":
            case "micros":
            case "microsecond":
            case "microseconds":
                return Duration.ofNanos(amount * 1000);
                
            case "ms":
            case "millis":
            case "millisecond":
            case "milliseconds":
                return Duration.ofMillis(amount);
                
            case "s":
            case "sec":
            case "second":
            case "seconds":
                return Duration.ofSeconds(amount);
                
            case "m":
            case "min":
            case "minute":
            case "minutes":
                return Duration.ofMinutes(amount);
                
            case "h":
            case "hr":
            case "hour":
            case "hours":
                return Duration.ofHours(amount);
                
            case "d":
            case "day":
            case "days":
                return Duration.ofDays(amount);
                
            default:
                return null; // Unknown unit
        }
    }
} 