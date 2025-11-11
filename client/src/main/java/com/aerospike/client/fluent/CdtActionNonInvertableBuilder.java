package com.aerospike.client.fluent;

/**
 * This interface defines the operations available at the end of a CDT path, other than
 * the set(). There are two fundamental operations, get() and remove() but there are 
 * different varieties of these based on the return type (eg count() and getValue() are
 * both get() operations.
 */
public interface CdtActionNonInvertableBuilder {
    public OperationBuilder getValues();
    public OperationBuilder getKeys();
    public OperationBuilder count();
    public OperationBuilder getIndex();
    public OperationBuilder getReverseIndex();
    public OperationBuilder getRank();
    public OperationBuilder getReverseRank();
    public OperationBuilder getKeyAndValue();
    public OperationBuilder remove();
}
