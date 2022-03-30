package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.operator.TableMeta.DataType;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

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

    /**
     * <pre>
     * Index values used by batch operators such {@link BatchHashJoin}.
     * And {@link Catalog}'s for extracting values used in
     * Index access
     *
     * MOTE! Important to implements hashCode/equals since these values might be put in cache
     * </pre>
     */
    interface IOrdinalValues
    {
        /** Get size of values. */
        int size();

        /** Get type of provided ordinal. */
        default DataType getType(int ordinal)
        {
            if (ordinal < size())
            {
                return DataType.ANY;
            }

            throw new IllegalArgumentException("Invalid ordinal");
        }

        /** Get value for provided ordinal */
        Object getValue(int ordinal);

        /** Get int value */
        default int getInt(int ordinal)
        {
            return (int) getValue(ordinal);
        }

        /** Get long value */
        default long getLong(int ordinal)
        {
            return (long) getValue(ordinal);
        }

        /** Get float value */
        default float getFloat(int ordinal)
        {
            return (float) getValue(ordinal);
        }

        /** Get double value */
        default double getDouble(int ordinal)
        {
            return (double) getValue(ordinal);
        }

        /** Get boolean value */
        default boolean getBool(int ordinal)
        {
            return (boolean) getValue(ordinal);
        }

        //CSOFF
        //@formatter:off
        default boolean isEquals(IOrdinalValues that)
        {
            // Since these classes are created by different class loaders,
            // we cannot use instanceof of. So make use of accessor methods on the interface instead
            int size = size();
            if (size != that.size())
            {
                return false;
            }
            for (int i = 0; i < size; i++)
            {
                DataType thisType = getType(i);
                DataType thatType = that.getType(i);

                /*
                 * NOTE!
                 * Code below is messy but needs to be done to avoid as much boxing as possible
                 * There are alot of use cases here that makes the code aweful
                 *  - Equal between numbers of different types (int = float, long = double etc.) should yield equal
                 *  - Joining on string and ints (1 = '1') should yield equal
                 *  - When haveing primitive on one side and ANY on the other side
                 *    reflection must be used and then also boxing, but in optimal queries
                 *    that code should never run.
                 */

                if (thisType == DataType.INT)
                {
                    int thisValue = getInt(i);
                    switch (thatType)
                    {
                        case ANY:
                            Object objValue = that.getValue(i);
                            if (objValue instanceof Integer) { if (thisValue != ((Integer) objValue).intValue()) { return false; } }
                            else if (objValue instanceof Long) { if (thisValue != ((Long) objValue).longValue()) { return false; } }
                            else if (objValue instanceof Float) { if (thisValue != ((Float) objValue).floatValue()) { return false; } }
                            else if (objValue instanceof Double) { if (thisValue != ((Double) objValue).doubleValue()) { return false; } }
                            else if (objValue instanceof Boolean) { if (thisValue != (((Boolean) objValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, objValue)) { return false; }
                            break;
                        case BOOLEAN:
                            if (thisValue != (that.getBool(i) ? 1 : 0)) { return false; }
                            break;
                        case DOUBLE:
                            if (thisValue != that.getDouble(i)) { return false; }
                            break;
                        case FLOAT:
                            if (thisValue != that.getFloat(i)) { return false; }
                            break;
                        case INT:
                            if (thisValue != that.getInt(i)) { return false; }
                            break;
                        case LONG:
                            if (thisValue != that.getLong(i)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
                else if (thisType == DataType.LONG)
                {
                    long thisValue = getLong(i);
                    switch (thatType)
                    {
                        case ANY:
                            Object objValue = that.getValue(i);
                            if (objValue instanceof Integer) { if (thisValue != ((Integer) objValue).intValue()) { return false; } }
                            else if (objValue instanceof Long) { if (thisValue != ((Long) objValue).longValue()) { return false; } }
                            else if (objValue instanceof Float) { if (thisValue != ((Float) objValue).floatValue()) { return false; } }
                            else if (objValue instanceof Double) { if (thisValue != ((Double) objValue).doubleValue()) { return false; } }
                            else if (objValue instanceof Boolean) { if (thisValue != (((Boolean) objValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, objValue)) { return false; }
                            break;
                        case BOOLEAN:
                            if (thisValue != (that.getBool(i) ? 1 : 0)) { return false; }
                            break;
                        case DOUBLE:
                            if (thisValue != that.getDouble(i)) { return false; }
                            break;
                        case FLOAT:
                            if (thisValue != that.getFloat(i)) { return false; }
                            break;
                        case INT:
                            if (thisValue != that.getInt(i)) { return false; }
                            break;
                        case LONG:
                            if (thisValue != that.getLong(i)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
                else if (thisType == DataType.FLOAT)
                {
                    float thisValue = getFloat(i);
                    switch (thatType)
                    {
                        case ANY:
                            Object objValue = that.getValue(i);
                            if (objValue instanceof Integer) { if (thisValue != ((Integer) objValue).intValue()) { return false; } }
                            else if (objValue instanceof Long) { if (thisValue != ((Long) objValue).longValue()) { return false; } }
                            else if (objValue instanceof Float) { if (thisValue != ((Float) objValue).floatValue()) { return false; } }
                            else if (objValue instanceof Double) { if (thisValue != ((Double) objValue).doubleValue()) { return false; } }
                            else if (objValue instanceof Boolean) { if (thisValue != (((Boolean) objValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, objValue)) { return false; }
                            break;
                        case BOOLEAN:
                            if (thisValue != (that.getBool(i) ? 1 : 0)) { return false; }
                            break;
                        case DOUBLE:
                            if (thisValue != that.getDouble(i)) { return false; }
                            break;
                        case FLOAT:
                            if (thisValue != that.getFloat(i)) { return false; }
                            break;
                        case INT:
                            if (thisValue != that.getInt(i)) { return false; }
                            break;
                        case LONG:
                            if (thisValue != that.getLong(i)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
                else if (thisType == DataType.DOUBLE)
                {
                    double thisValue = getDouble(i);
                    switch (thatType)
                    {
                        case ANY:
                            Object objValue = that.getValue(i);
                            if (objValue instanceof Integer) { if (thisValue != ((Integer) objValue).intValue()) { return false; } }
                            else if (objValue instanceof Long) { if (thisValue != ((Long) objValue).longValue()) { return false; } }
                            else if (objValue instanceof Float) { if (thisValue != ((Float) objValue).floatValue()) { return false; } }
                            else if (objValue instanceof Double) { if (thisValue != ((Double) objValue).doubleValue()) { return false; } }
                            else if (objValue instanceof Boolean) { if (thisValue != (((Boolean) objValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, objValue)) { return false; }
                            break;
                        case BOOLEAN:
                            if (thisValue != (that.getBool(i) ? 1 : 0)) { return false; }
                            break;
                        case DOUBLE:
                            if (thisValue != that.getDouble(i)) { return false; }
                            break;
                        case FLOAT:
                            if (thisValue != that.getFloat(i)) { return false; }
                            break;
                        case INT:
                            if (thisValue != that.getInt(i)) { return false; }
                            break;
                        case LONG:
                            if (thisValue != that.getLong(i)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
                else if (thisType == DataType.BOOLEAN)
                {
                    boolean thisValue = getBool(i);
                    int thisIntValue = thisValue ? 1 : 0;
                    switch (thatType)
                    {
                        case ANY:
                            Object objValue = that.getValue(i);
                            if (objValue instanceof Boolean) { if ( thisValue != ((Boolean) objValue).booleanValue()) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, objValue)) { return false; }
                            break;
                        case BOOLEAN:
                            if (thisValue != that.getBool(i)) { return false; }
                            break;
                        case DOUBLE:
                            if (thisIntValue != that.getDouble(i)) { return false; }
                            break;
                        case FLOAT:
                            if (thisIntValue != that.getFloat(i)) { return false; }
                            break;
                        case INT:
                            if (thisIntValue != that.getInt(i)) { return false; }
                            break;
                        case LONG:
                            if (thisIntValue != that.getLong(i)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
                else if (thisType == DataType.ANY)
                {
                    Object thisValue = getValue(i);
                    switch (thatType)
                    {
                        case ANY:
                            if (!ExpressionMath.eq(thisValue, that.getValue(i))) { return false; }
                            break;
                        case BOOLEAN:
                            boolean boolValue = that.getBool(i);
                            int intBoolValue = boolValue ? 1 : 0;
                            if (thisValue instanceof Integer) { if ( intBoolValue != ((Integer) thisValue).intValue()) { return false; } }
                            else if (thisValue instanceof Long) { if ( intBoolValue != ((Long) thisValue).longValue()) { return false; } }
                            else if (thisValue instanceof Float) { if ( intBoolValue != ((Float) thisValue).floatValue()) { return false; } }
                            else if (thisValue instanceof Double) { if ( intBoolValue != ((Double) thisValue).doubleValue()) { return false; } }
                            else if (thisValue instanceof Boolean) { if ( boolValue != ((Boolean) thisValue).booleanValue()) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, boolValue)) { return false; }
                            break;
                        case DOUBLE:
                            double doubleValue = that.getDouble(i);
                            if (thisValue instanceof Integer) { if ( doubleValue != ((Integer) thisValue).intValue()) { return false; } }
                            else if (thisValue instanceof Long) { if ( doubleValue != ((Long) thisValue).longValue()) { return false; } }
                            else if (thisValue instanceof Float) { if ( doubleValue != ((Float) thisValue).floatValue()) { return false; } }
                            else if (thisValue instanceof Double) { if ( doubleValue != ((Double) thisValue).doubleValue()) { return false; } }
                            else if (thisValue instanceof Boolean) { if ( doubleValue != (((Boolean) thisValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, doubleValue)) { return false; }
                            break;
                        case FLOAT:
                            float floatValue = that.getFloat(i);
                            if (thisValue instanceof Integer) { if ( floatValue != ((Integer) thisValue).intValue()) { return false; } }
                            else if (thisValue instanceof Long) { if ( floatValue != ((Long) thisValue).longValue()) { return false; } }
                            else if (thisValue instanceof Float) { if ( floatValue != ((Float) thisValue).floatValue()) { return false; } }
                            else if (thisValue instanceof Double) { if ( floatValue != ((Double) thisValue).doubleValue()) { return false; } }
                            else if (thisValue instanceof Boolean) { if ( floatValue != (((Boolean) thisValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, floatValue)) { return false; }
                            break;
                        case INT:
                            int intValue = that.getInt(i);
                            if (thisValue instanceof Integer) { if ( intValue != ((Integer) thisValue).intValue()) { return false; } }
                            else if (thisValue instanceof Long) { if ( intValue != ((Long) thisValue).longValue()) { return false; } }
                            else if (thisValue instanceof Float) { if ( intValue != ((Float) thisValue).floatValue()) { return false; } }
                            else if (thisValue instanceof Double) { if ( intValue != ((Double) thisValue).doubleValue()) { return false; } }
                            else if (thisValue instanceof Boolean) { if ( intValue != (((Boolean) thisValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, intValue)) { return false; }
                            break;
                        case LONG:
                            long longValue = that.getLong(i);
                            if (thisValue instanceof Integer) { if ( longValue != ((Integer) thisValue).intValue()) { return false; } }
                            else if (thisValue instanceof Long) { if ( longValue != ((Long) thisValue).longValue()) { return false; } }
                            else if (thisValue instanceof Float) { if ( longValue != ((Float) thisValue).floatValue()) { return false; } }
                            else if (thisValue instanceof Double) { if ( longValue != ((Double) thisValue).doubleValue()) { return false; } }
                            else if (thisValue instanceof Boolean) { if ( longValue != (((Boolean) thisValue).booleanValue() ? 1 : 0)) { return false; } }
                            else if (!ExpressionMath.eq(thisValue, longValue)) { return false; }
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown type " + thatType);
                    }
                }
            }
            return true;
        }
        //CSON
        //@formatter:on
    }
}
