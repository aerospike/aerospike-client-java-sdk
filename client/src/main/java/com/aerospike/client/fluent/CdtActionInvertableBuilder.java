package com.aerospike.client.fluent;

/**
 * This interface allows actions at the end of a CDT path but includes only the actions
 * which are valid to pass INVERTED to. This excludes items which return one result -
 * MapOperation.getByKey, mapOperation.getByIndex, mapOperation.getByRank. These operations 
 * will throw a ParameterError if you try to invoke them with the INVERTED flag. This
 * interface is only returned after a context call which selects multiple elements.
 * <p>
 * These methods use the INVERTED flag to return all elements EXCEPT those selected:
 * <ul>
 *   <li>VALUE | INVERTED → getAllOtherValues()</li>
 *   <li>KEY | INVERTED → getAllOtherKeys()</li>
 *   <li>COUNT | INVERTED → countAllOthers()</li>
 *   <li>INDEX | INVERTED → getAllOtherIndexes()</li>
 *   <li>REVERSE_INDEX | INVERTED → getAllOtherReverseIndexes()</li>
 *   <li>RANK | INVERTED → getAllOtherRanks()</li>
 *   <li>REVERSE_RANK | INVERTED → getAllOtherReverseRanks()</li>
 *   <li>KEY_VALUE | INVERTED → getAllOtherKeysAndValues()</li>
 *   <li>INVERTED → removeAllOthers()</li>
 * </ul>
 */
public interface CdtActionInvertableBuilder<T extends AbstractOperationBuilder<T>> extends CdtActionNonInvertableBuilder<T> {
    public T getAllOtherValues();
    public T getAllOtherKeys();
    public T countAllOthers();
    public T getAllOtherIndexes();
    public T getAllOtherReverseIndexes();
    public T getAllOtherRanks();
    public T getAllOtherReverseRanks();
    public T getAllOtherKeysAndValues();
    public T removeAllOthers();

}