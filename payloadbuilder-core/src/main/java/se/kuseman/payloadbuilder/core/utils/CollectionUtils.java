package se.kuseman.payloadbuilder.core.utils;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/** Collection utils */
public final class CollectionUtils
{
    private CollectionUtils()
    {
    }

    /** Create set from provided items */
    @SafeVarargs
    public static <T> Set<T> asSet(T... items)
    {
        return new HashSet<>(asList(items));
    }

    /** Create set from provided items */
    @SafeVarargs
    public static <T> Set<T> asOrderedSet(T... items)
    {
        return new LinkedHashSet<>(asList(items));
    }
}
