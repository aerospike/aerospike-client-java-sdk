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
package com.aerospike.client.sdk.util;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.aerospike.client.sdk.AerospikeException;

public final class Util {
    private static final Pattern SIZE_PATTERN = Pattern.compile("^(\\d+)\\s*([a-zA-Z]*)$");

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException ie) {
        }
    }

    public static String getErrorMessage(Throwable e) {
        // Connection error messages don't need a stacktrace.
        Throwable cause = e.getCause();
        if (e instanceof SocketException || e instanceof AerospikeException.Connection ||
            cause instanceof SocketTimeoutException) {
            return e.getMessage();
        }

        if (e instanceof EOFException || cause instanceof EOFException) {
            return EOFException.class.getName();
        }

        // Unexpected exceptions need a stacktrace.
        return getStackTrace(e);
    }

    public static String getStackTrace(Throwable e) {
        StringWriter sw = new StringWriter(1000);
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static byte[] readFile(File file) {
        try {
            byte[] bytes = new byte[(int)file.length()];

            try (FileInputStream in = new FileInputStream(file)) {
                int pos = 0;
                int len = 0;

                while (pos < bytes.length) {
                    len = in.read(bytes, pos, bytes.length - pos);
                    pos += len;
                }
                return bytes;
            }
        }
        catch (Throwable e) {
            throw new AerospikeException("Failed to read " + file.getAbsolutePath(), e);
        }
    }

    public static byte[] readResource(ClassLoader resourceLoader, String resourcePath) {
        try {
            URL url = resourceLoader.getResource(resourcePath);

            if (url == null) {
                throw new IllegalArgumentException("Resource: " + resourcePath + " not found");
            }

            try (InputStream is = url.openStream()) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream(8192)) {
                    byte[] bytes = new byte[8192];
                    int length;

                    while ((length = is.read(bytes)) > 0) {
                        bos.write(bytes, 0, length);
                    }
                    return bos.toByteArray();
                }
            }
        }
        catch (Throwable e) {
            throw new AerospikeException("Failed to read resource " + resourcePath, e);
        }
    }

    /**
     * Convert a comma separated array of strings to a BigInteger array.
     * Each individual string will be treated as hex if the string prefix is "0x".
     */
    public static BigInteger[] toBigIntegerArray(String str) {
        String[] strArray = str.split(",");
        BigInteger[] bigArray = new BigInteger[strArray.length];
        int count = 0;

        for (String s : strArray) {
            if (s.startsWith("0x")) {
                bigArray[count] = new BigInteger(s.substring(2), 16);
            }
            else if (s.indexOf(':') >= 0) {
                // Some certificates show serial numbers in hex pairs delimited by colons.
                // Remove those colons before converting to BigInteger.
                s = s.replaceAll(":", "");
                bigArray[count] = new BigInteger(s, 16);
            }
            else {
                bigArray[count] = new BigInteger(s);
            }
            count++;
        }
        return bigArray;
    }

    /**
     * Convert a string to a time stamp using the same algorithm as the Aerospike loader.
     */
    public static long toTimeStamp(String dateTime, SimpleDateFormat format, int timeZoneOffset) throws ParseException {
        Date formatDate = format.parse(dateTime);
        long miliSecondForDate = formatDate.getTime()
                - timeZoneOffset;
        return miliSecondForDate / 1000;
    }

    /**
     * Convert a string to a time stamp using a string pattern.
     */
    public static long toTimeStamp(String dateTime, String pattern, int timeZoneOffset) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return toTimeStamp(dateTime, format, timeZoneOffset);
    }

    /**
     * Convert a time stamp (time in milliseconds) to a string.
     */
    public static String fromTimeStamp(long timeStamp, SimpleDateFormat format){
        Date formatDate = new Date(timeStamp);
        return format.format(formatDate);
    }

    /**
     * Convert a time stamp (time in milliseconds) to a string using a string pattern.
     */
    public static String fromTimeStamp(long timeStamp, String pattern){
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return fromTimeStamp(timeStamp, format);
    }

    /**
     * Parse a data size string into a number of bytes. The value is a non-negative
     * integer followed by an optional, case-insensitive unit: {@code b}, {@code k}/{@code kb},
     * {@code m}/{@code mb}, {@code g}/{@code gb} or {@code t}/{@code tb}. Units are binary
     * (1 KB = 1024 bytes). Whitespace between the number and unit is allowed.
     * Examples: {@code "10mb"}, {@code "512 KB"}, {@code "1g"}, {@code "4096"}.
     *
     * @param value size string
     * @return size in bytes
     * @throws IllegalArgumentException if the value is null or cannot be parsed
     */
    public static long parseSize(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Size value is null");
        }

        Matcher m = SIZE_PATTERN.matcher(value.trim());

        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid size: '" + value +
                "'. Expected <number>[b|k|kb|m|mb|g|gb|t|tb] (e.g. '10mb')");
        }

        String unit = m.group(2).toLowerCase();
        int shift;

        switch (unit) {
            case "":
            case "b":
                shift = 0;
                break;
            case "k":
            case "kb":
                shift = 10;
                break;
            case "m":
            case "mb":
                shift = 20;
                break;
            case "g":
            case "gb":
                shift = 30;
                break;
            case "t":
            case "tb":
                shift = 40;
                break;
            default:
                throw new IllegalArgumentException("Invalid size unit '" + m.group(2) +
                    "' in '" + value + "'. Expected b, k, kb, m, mb, g, gb, t or tb");
        }

        try {
            return Math.multiplyExact(Long.parseLong(m.group(1)), 1L << shift);
        }
        catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException("Size out of range: '" + value + "'", e);
        }
    }

    /**
     * Convert object returned from server to long.
     */
    public static long toLong(Object obj) {
        // The server always returns numbers as longs if found.
        // If not found, the server may return null.  Convert null to zero.
        return (obj != null)? (Long)obj : 0;
    }

    /**
     * Convert object returned from server to int.
     */
    public static int toInt(Object obj) {
        // The server always returns numbers as longs, so get long and cast.
        return (int)toLong(obj);
    }

    /**
     * Convert object returned from server to short.
     */
    public static short toShort(Object obj) {
        // The server always returns numbers as longs, so get long and cast.
        return (short)toLong(obj);
    }

    /**
     * Convert object returned from server to byte.
     */
    public static byte toByte(Object obj) {
        // The server always returns numbers as longs, so get long and cast.
        return (byte)toLong(obj);
    }

    /**
     * Convert object returned from server to boolean.
     */
    public static boolean toBoolean(Object obj) {
        // The server always returns booleans as longs, so get long and convert.
        return (toLong(obj) != 0) ? true : false;
    }

    /**
     * Return cpu usage percent of this process.
     */
    public static double getProcessCpuLoad() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

            if (list.isEmpty()) {
                return 0.0;
            }

            Attribute att = (Attribute)list.get(0);
            Double value = (Double)att.getValue();

            // usually takes a couple of seconds before we get real values
            if (value == -1.0) {
                return 0.0;
            }

            // returns a percentage value with 1 decimal point precision
            return ((int)(value * 1000) / 10.0);
        }
        catch (Throwable e) {
            return 0.0;
        }
    }

    public static boolean rackIdsEqual(List<Integer> racks1, int[] racks2) {
        if (racks1 == null) {
            return racks2 == null;
        }
        else if (racks2 == null) {
            return false;
        }

        if (racks1.size() != racks2.length) {
            return false;
        }

        for (int i = 0; i < racks2.length; i++) {
            int r1 = racks1.get(i);
            int r2 = racks2[i];

            if (r1 != r2) {
                return false;
            }
        }
        return true;
    }
}
