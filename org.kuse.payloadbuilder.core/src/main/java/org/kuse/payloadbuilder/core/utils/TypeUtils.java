package org.kuse.payloadbuilder.core.utils;

import org.kuse.payloadbuilder.core.operator.TableMeta.DataType;

/** Type utils */
public final class TypeUtils
{
    private TypeUtils()
    {
    }

    /** Returns true if types are native comparable without reflection */
    public static boolean isNativeComparable(DataType typeA, DataType typeB)
    {
        return typeA.promote(typeB) != null;
    }

    /**
     * Returns the arithmetic data type to use for provided types. Promotes types if needed.
     *
     * @return Type used or Object.class if no type could be found
     */
    public static DataType getArithmeticDataType(DataType typeA, DataType typeB)
    {
        if (typeA == typeB)
        {
            return typeA;
        }

        DataType promoted = typeA.promote(typeB);
        if (promoted == null)
        {
            return DataType.ANY;
        }
        return promoted;
    }
}
