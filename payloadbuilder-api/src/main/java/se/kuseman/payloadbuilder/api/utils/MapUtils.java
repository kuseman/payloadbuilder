package se.kuseman.payloadbuilder.api.utils;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** Map utilities */
public class MapUtils
{
    private MapUtils()
    {
    }

    /** Build map out of entries */
    @SafeVarargs
    public static <K, V> Map<K, V> ofEntries(SimpleEntry<K, V>... values)
    {
        return ofEntries(false, values);
    }

    /** Build a map with kept insertion order out of entries */
    @SafeVarargs
    public static <K, V> Map<K, V> ofEntries(boolean keepInsertOrder, SimpleEntry<K, V>... values)
    {
        Map<K, V> result = keepInsertOrder ? new LinkedHashMap<>()
                : new HashMap<>();

        for (SimpleEntry<K, V> e : values)
        {
            if (result.put(e.getKey(), e.getValue()) != null)
            {
                throw new IllegalStateException(String.format("Duplicate key %s", e.getKey()));
            }
        }
        return result;
    }

    /** Build entry out of provided key and value */
    public static <K, V> SimpleEntry<K, V> entry(K key, V value)
    {
        return new SimpleEntry<>(key, value);
    }
}
