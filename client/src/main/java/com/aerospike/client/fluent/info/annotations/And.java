package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
/**
 * For boolean fields, merge values from different nodes together by ANDing them.
 * Quite often this will return the worst result of positive elements
 * <p/>
 * For example, this could be used to determine if {@code prefer-uniform-balance} is
 * set on all nodes in the cluster; if any one node returns {@code false}, this will return {@code false}
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface And {

}
