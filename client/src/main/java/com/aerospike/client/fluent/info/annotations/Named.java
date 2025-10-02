package com.aerospike.client.fluent.info.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sometimes the metric/config name will not exactly match the object name. In this case, use 
 * {@code @Named} to specify the metric/config value to map.
 * <p/>
 * For example, sindex information contains "indexname", but it is mapped to "indexName" via:
 * <pre>
 *    @Named("indexname")
 *    private String indexName;
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Named {
    String value();
}
