package com.aerospike.client.fluent;

/**
 * The TypeSafeDataSet allows object mapping functionality without warnings.
 * eg 
 *     session.insert(customerDataSet).object(sampleCust).execute();
 * gives an usafe type compiler warning if just using a DataSet.
 * 
 * TODO: See if this class can be removed and still eliminate the warnings.
 * @param <T>
 */
public class TypeSafeDataSet<T> extends DataSet {
    private final Class<T> clazz;
    public TypeSafeDataSet(String namespace, String set, Class<T> clazz) {
        super(namespace, set);
        this.clazz = clazz;
    }

    public static <R> TypeSafeDataSet<R> of(String namespace, String set, Class<R> clazz) {
        return new TypeSafeDataSet<>(namespace, set, clazz);
    }
    
    public Class<T> getClazz() {
        return clazz;
    }
}
