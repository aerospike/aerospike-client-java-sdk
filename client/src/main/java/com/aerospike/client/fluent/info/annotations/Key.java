package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This field is a key field for merging records together. If two objects have identical key fields, they 
 * will be interpreted as the same object and have the values merged together. When there are multiple nodes in the
 * cluster, they will all report metrics for the same entity, and these need to be merged together.
 * <p/>
 * For example:
 * <ul>
 * <li>Set details must have the name {@code ns} and {@code set} fields to be the same</li>
 * <li>Sindex details must have the name {@code ns}, {@code set}, {@code bin} and {@code indexname} fields to be the same</li>
 * </ul>
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface Key {

}
