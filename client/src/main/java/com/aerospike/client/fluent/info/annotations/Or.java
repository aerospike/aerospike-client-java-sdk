package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * For boolean fields, merge values from different nodes together by ORing them.
 * Quite often this will return the worst result of negative elements
 * <p/>
 * For example, this could be used to determine if {@code stop-writes} is
 * set on any node in the cluster; if any one node returns {@code true}, this will return {@code true}
 */

@Retention(RUNTIME)
@Target(FIELD)
public @interface Or {

}
