package com.aerospike.client.fluent.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Return the most common value across all passed values. If there are multiple most 
 * common values, one of the values with the most common value will be returned.
 * <p/>
 * For example, if the list is [1,3,5,6,3,1,3,7,5,1,4] then either 1 or 3 would be returned
 * <p/>
 * This is the default for Enum types
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MostCommon {
}
