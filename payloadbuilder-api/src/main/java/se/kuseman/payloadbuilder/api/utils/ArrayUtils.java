package se.kuseman.payloadbuilder.api.utils;

import java.lang.reflect.Array;

/** Array utils */
public class ArrayUtils
{
    private static final int INDEX_NOT_FOUND = -1;

    /** Apache commons lang3 */
    public static int indexOf(Object[] array, Object elementToFind)
    {
        return indexOf(array, elementToFind, 0);
    }

    /** Apache commons lang3 */
    public static int indexOf(final Object[] array, final Object objectToFind, int startIndex)
    {
        if (array == null)
        {
            return INDEX_NOT_FOUND;
        }
        if (startIndex < 0)
        {
            startIndex = 0;
        }
        if (objectToFind == null)
        {
            for (int i = startIndex; i < array.length; i++)
            {
                if (array[i] == null)
                {
                    return i;
                }
            }
        }
        else
        {
            for (int i = startIndex; i < array.length; i++)
            {
                if (objectToFind.equals(array[i]))
                {
                    return i;
                }
            }
        }
        return INDEX_NOT_FOUND;
    }

    /** Apache commons lang3 */
    public static boolean isEmpty(final Object[] array)
    {
        return getLength(array) == 0;
    }

    /** Apache commons lang3 */
    public static boolean isEmpty(final char[] array)
    {
        return getLength(array) == 0;
    }

    /** Apache commons lang3 */
    public static int getLength(final Object array)
    {
        if (array == null)
        {
            return 0;
        }
        return Array.getLength(array);
    }
}
