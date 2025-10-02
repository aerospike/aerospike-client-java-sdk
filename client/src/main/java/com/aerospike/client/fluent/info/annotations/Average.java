package com.aerospike.client.fluent.info.annotations;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * For numeric fields, merge values from different nodes together by averaging them. This is the default for double fields.
 * <p/>
 * For example, this could be used on the {@code data_compression_ratio} field
 */

@Retention(RUNTIME)
@Target(FIELD)
public @interface Average {

}
