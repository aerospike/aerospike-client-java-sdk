package com.aerospike.client.fluent.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Some of the config/metrics are poorly named. For example, there is a "storage-engine" config which
 * can be "memory" or "device", but then there are configs like "storage-engine.defrag-lwm-pct" 
 * which implies that storage-engine is an object in its own right. We cannot change these definitions,
 * but rather we intercept the metrics and rename them to what makes the most sense for parsing them.
 * <p/>
 * In this example, we would use {@code @Mapping(from = "storage-engine", to = "storage-engine.type")} to
 * allow the type to be populated on the storage engine object.
 * <p/>
 * Note that the {@code from} parameter can be a regular expression, and any placeholders matched
 * in this expression can be used by ordinal number in the {@code to} expression.
 * <p/>
 * For example: {@code @Mapping(from = "storage-engine.file\\[(\\d+)\\]", to = "storage-engine.files[$1].filePath")}
 * <p/>
 * Note that mappings can be used directly on a class, or if you need multiple mappings, use the {@link Mappings} class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Mapping {
    String from();
    String to();
}
