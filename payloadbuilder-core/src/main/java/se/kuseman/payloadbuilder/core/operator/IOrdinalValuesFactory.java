package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/**
 * Factory that creates {@link IOrdinalValues}.
 *
 * <pre>
 * This factory creates objects that is used as key in maps for example BatchhashJoin, GroupBy
 * Also used as outer value in index access in catalogs to let catalog access values in their indices
 *
 * Ex.
 *
 * Index on tableB (col1, col2)
 *
 * select *
 * from tableA a
 * inner join tableB b
 *  on b.col1 = a.col1
 *  and b.col2 = @value
 *
 * Here we will create {@link IOrdinalValues} for each (a.col1, @value) pair
 * that will be accessible by tableB operator to index into it's rows.
 * </pre>
 */
public interface IOrdinalValuesFactory
{
    /** Create an index values instance from provided tuple */
    IOrdinalValues create(ExecutionContext context, Tuple tuple);

    /** Returns the size of the values create by this factory */
    int size();
}
