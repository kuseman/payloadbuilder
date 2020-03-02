package com.viskan.payloadbuilder.utils;

import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

/** Map utilities */
public class MapUtils
{
    private MapUtils()
    {
    }

    @SafeVarargs
    public static <K, V> Map<K, V> ofEntries(SimpleEntry<K, V>... values)
    {
        return Stream.of(values).collect(toMap(Entry::getKey, Entry::getValue));
    }

    public static <K, V> SimpleEntry<K, V> entry(K key, V value)
    {
        return new SimpleEntry<>(key, value);
    }
}
