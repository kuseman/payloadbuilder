package com.viskan.payloadbuilder.utils;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Set;

public final class CollectionUtils
{
    private CollectionUtils()
    {}
    
    /** Create set from provided items */
    @SafeVarargs
    public static <T> Set<T> asSet(T ...items)
    {
        return new HashSet<>(asList(items));
    }
}
