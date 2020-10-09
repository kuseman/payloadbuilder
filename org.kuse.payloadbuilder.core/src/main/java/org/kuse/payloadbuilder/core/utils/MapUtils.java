package org.kuse.payloadbuilder.core.utils;

import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

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
        return Stream.of(values)
                .collect(toMap(
                        Entry::getKey,
                        Entry::getValue,
                        (u, v) ->
                        {
                            throw new IllegalStateException(String.format("Duplicate key %s", u));
                        },
                        keepInsertOrder ? LinkedHashMap::new : HashMap::new));
    }

    /** Build entry out of provided key and value */
    public static <K, V> SimpleEntry<K, V> entry(K key, V value)
    {
        return new SimpleEntry<>(key, value);
    }

    /** Traverse map out of provided parts */
    public static Object traverse(Map<Object, Object> map, List<String> parts)
    {
        return traverse(map, 0, parts);
    }

    /**
     * <pre>
     * Traverses a map by keys provided. Assumes map type all the way down to parts.size - 1, if not null is returned
     * </p>
     *
     * @param map Map to traverse
     * @param startIndex Index to start traverse from in #parts
     * @param parts Parts to traverse
     */
    @SuppressWarnings("unchecked")
    public static Object traverse(Map<Object, Object> map, int startIndex, List<String> parts)
    {
        Map<Object, Object> current = map;
        int size = parts.size();
        for (int i = startIndex; i < size - 1; i++)
        {
            Object o = current.get(parts.get(i));
            if (!(o instanceof Map))
            {
                return null;
            }
            current = (Map<Object, Object>) o;
        }

        return current.get(parts.get(size - 1));
    }
}
