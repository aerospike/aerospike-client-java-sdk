package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * For numeric fields, merge values from different nodes together by aggregating them. This is the default for integer fields.
 * <p/>
 * For example, this could be used on the number of objects in a namespace.
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface Aggregate {

}
