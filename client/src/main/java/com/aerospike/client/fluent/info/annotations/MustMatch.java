package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Throw an exception if node do not all agree on the same value. For example replication-factor
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface MustMatch {

}
