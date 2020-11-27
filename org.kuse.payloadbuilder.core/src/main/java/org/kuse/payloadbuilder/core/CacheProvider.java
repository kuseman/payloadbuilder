package org.kuse.payloadbuilder.core;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;

/** Definition of a cache provider. Used by operators that supports caching
 * to cache/retrieve tuples */
public interface CacheProvider
{
    /** Get all cached values for provided keys.
     * NOTE! All keys should yield a map entry in result whether
     * a cached value was found or not.
     * */
    Map<Object, List<Tuple>> getAll(List<Object> keys);
}
