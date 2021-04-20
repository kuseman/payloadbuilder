package org.kuse.payloadbuilder.core.utils;

import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;

/** Type utils */
public class TypeUtils
{
//    private static final Map<Da, Integer> TYPE_BY_PRIO = MapUtils.ofEntries(
//            MapUtils.entry(int.class, 0),
//            MapUtils.entry(Integer.class, 0),
//            MapUtils.entry(long.class, 1),
//            MapUtils.entry(Long.class, 1),
//            MapUtils.entry(float.class, 2),
//            MapUtils.entry(Float.class, 2),
//            MapUtils.entry(double.class, 3),
//            MapUtils.entry(Double.class, 3));

    private TypeUtils()
    {
    }

//    /**
//     * Returns true if the types are the same regarding type in code generation environment. Ie a Long.class is the same as long.class value wise.
//     */
//    public static boolean isSameType(DataType typeA, DataType typeB)
//    {
//        if (typeA == typeB || EQUAL_VALUE_TYPES.get(typeA) == typeB)
//        {
//            return true;
//        }
//
//        return false;
//    }

//    /**
//     * Return type string for provided type. Used in code generation. For example Long and long will have the same type in generated code.
//     */
//    public static String getTypeString(Class<?> type)
//    {
//        if (type == int.class
//            || type == Integer.class)
//        {
//            return "int";
//        }
//        else if (type == long.class
//            || type == Long.class)
//        {
//            return "long";
//        }
//        else if (type == float.class
//            || type == Float.class)
//        {
//            return "float";
//        }
//        else if (type == double.class
//            || type == Double.class)
//        {
//            return "double";
//        }
//        else if (type == boolean.class
//            || type == Boolean.class)
//        {
//            return "boolean";
//        }
//
//        return "Object";
//    }

    /** Returns true if types are native comparable without reflection */
    public static boolean isNativeComparable(DataType typeA, DataType typeB)
    {
        return typeA.promote(typeB) != null;
    }

//    /** Return true if provided types are booleans */
//    public static boolean isBooleans(Class<?> typeA, Class<?> typeB)
//    {
//        return isBoolean(typeA) && isBoolean(typeB);
//    }

//    /** Return true if provided type are boolean */
//    public static boolean isBoolean(Class<?> type)
//    {
//        return type == boolean.class || type == Boolean.class;
//    }

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
