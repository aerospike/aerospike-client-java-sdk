package com.aerospike.client.fluent.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Takes a list of string values, and when merging multiple values
 * returns the earliest entry in the list which has at least one instance in the merging values.  
 * <p>
 * For example merging index statuses with
 * <RW, RW, RW, WO, RW> with @FirstOf({"WO", "RW"}) will return "WO"
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FirstOf {
    String[] value(); // preferred order of enum names
}
