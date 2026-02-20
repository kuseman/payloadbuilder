package se.kuseman.payloadbuilder.bytes;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/**
 * Caches the result of reconciling an expected {@link Schema} against a payload's actual, drifted schema shape - so repeated reads of payloads sharing the same old/new shape (the common case when old
 * data is kept around rather than rewritten on every schema change) don't redo the reconciliation work every time.
 *
 * <p>
 * A single instance can be shared across every schema/table an application reads, for its whole lifetime - construct it once, eg. as a long-lived field, and pass it into every
 * {@link PayloadReader#readTupleVector(byte[], Schema, boolean, SchemaReconciliationCache)} call regardless of which table it's for. Entries are keyed on the expected {@link Schema}'s object
 * <b>identity</b> (not content equality) combined with a hash of the payload's type-tree bytes, so different schemas naturally partition into separate entries within the one cache - there's no need
 * for a cache-per-table registry. The schema doesn't need to (and normally won't) ever change for this to help; its identity is in the key purely so that sharing one cache instance across multiple
 * schemas can't cross-contaminate their reconciled results. The one thing that does matter: reuse the exact same {@link Schema} instance across reads for a given table - a freshly constructed but
 * merely equal Schema instance on every call will never hit the cache, since identity, not equality, is compared.
 * </p>
 *
 * <p>
 * Thread-safe. Bounded to {@code maxSize} distinct entries across all schemas sharing this cache - once exceeded, further reconciliation results simply aren't cached (existing entries are kept,
 * nothing is evicted) rather than evicting anything, since the realistic number of distinct historical schema shapes an application will ever encounter, across all its tables combined, is small and
 * human-driven (one per schema version ever deployed), so hitting the cap at all should be unusual. {@link #size()} lets a caller monitor for that.
 * </p>
 */
public class SchemaReconciliationCache
{
    private final int maxSize;
    private final ConcurrentHashMap<Key, ResolvedType> cache = new ConcurrentHashMap<>();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public SchemaReconciliationCache(int maxSize)
    {
        if (maxSize <= 0)
        {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        this.maxSize = maxSize;
    }

    /** Look up a previously reconciled schema for the given expected schema/type-tree fingerprint/expand combination. */
    ResolvedType get(Schema expectedSchema, long typeTreeHash, boolean expand)
    {
        ResolvedType value = cache.get(new Key(expectedSchema, typeTreeHash, expand));
        if (value != null)
        {
            hits.increment();
        }
        else
        {
            misses.increment();
        }
        return value;
    }

    /** Store a freshly reconciled schema. No-op once {@code maxSize} distinct entries are already cached. */
    void put(Schema expectedSchema, long typeTreeHash, boolean expand, ResolvedType value)
    {
        if (cache.size() >= maxSize)
        {
            return;
        }
        cache.put(new Key(expectedSchema, typeTreeHash, expand), value);
    }

    /** Number of distinct reconciled schema shapes currently cached. */
    public int size()
    {
        return cache.size();
    }

    /** Number of times a previously reconciled schema shape was found in the cache. */
    public long hitCount()
    {
        return hits.sum();
    }

    /** Number of times reconciliation had to run fresh because nothing matching was cached yet. */
    public long missCount()
    {
        return misses.sum();
    }

    /** Discard all cached entries. */
    public void clear()
    {
        cache.clear();
    }

    private static final class Key
    {
        private final Schema expectedSchema;
        private final long typeTreeHash;
        private final boolean expand;

        Key(Schema expectedSchema, long typeTreeHash, boolean expand)
        {
            this.expectedSchema = expectedSchema;
            this.typeTreeHash = typeTreeHash;
            this.expand = expand;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(System.identityHashCode(expectedSchema), typeTreeHash, expand);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this)
            {
                return true;
            }
            else if (obj == null)
            {
                return false;
            }
            if (!(obj instanceof Key that))
            {
                return false;
            }
            // Identity comparison on the schema is intentional - see class javadoc
            return expectedSchema == that.expectedSchema
                    && typeTreeHash == that.typeTreeHash
                    && expand == that.expand;
        }
    }
}
