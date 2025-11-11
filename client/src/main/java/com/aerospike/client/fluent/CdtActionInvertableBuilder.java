package com.aerospike.client.fluent;

/**
 * This interface allows actions at the end of a CDT path but includes only the actions
 * which are valid to pass INVERTED to. This excludes items which return one result -
 * MapOperation.getByKey, mapOperation.getByIndex, mapOperation.getByRank. These operations 
 * will throw a ParameterError if you try to invoke them with the INVERTED flag. This
 * interface is only returned after a context call which is selects multiple elements.
 */
public interface CdtActionInvertableBuilder extends CdtActionNonInvertableBuilder {
    public OperationBuilder getAllOtherValues();
    public OperationBuilder getAllOtherKeys();
    public OperationBuilder countAllOthers();
    public OperationBuilder getAllOtherIndexes();
    public OperationBuilder getAllOtherReverseIndexes();
    public OperationBuilder getAllOtherRanks();
    public OperationBuilder getAllOtherReverseRanks();
    public OperationBuilder getAllOtherKeysAndValues();
    public OperationBuilder removeAllOthers();

}