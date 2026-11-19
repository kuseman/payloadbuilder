package se.kuseman.payloadbuilder.catalog.mongodb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Utils for fetching (and caching) MongoDB collection/index metadata. */
final class MongoMetaUtils
{
    private static final QualifiedName CACHE_NAME = QualifiedName.of("Mongo", "Meta");
    private static final int DEFAULT_META_CACHE_TTL_MINUTES = 60;

    private MongoMetaUtils()
    {
    }

    /**
     * Return the single-field, non special (btree) indexes for provided table. NOTE! Compound indexes are discovered by MongoDB but not exposed here - v1 only supports seeking single field indexes,
     * mirroring the Elasticsearch catalog's own single-column-index restriction.
     */
    static List<MongoIndex> getIndexes(IQuerySession session, String catalogAlias, MongoClient client, MongoTable table)
    {
        ValueVector ttlProperty = session.getCatalogProperty(catalogAlias, MongoCatalog.CACHE_META_TTL_KEY);
        int ttl = (ttlProperty == null
                || ttlProperty.isNull(0)) ? DEFAULT_META_CACHE_TTL_MINUTES
                        : ttlProperty.getInt(0);
        QualifiedName key = QualifiedName.of(MongoClientHolder.getConnectionString(session, catalogAlias), table.database(), table.collection(), "indexes");
        return session.getGenericCache()
                .computIfAbsent(CACHE_NAME, key, Duration.ofMinutes(ttl), () -> fetchIndexes(client, table));
    }

    private static List<MongoIndex> fetchIndexes(MongoClient client, MongoTable table)
    {
        MongoCollection<Document> collection = client.getDatabase(table.database())
                .getCollection(table.collection());

        List<MongoIndex> result = new ArrayList<>();
        for (Document indexDoc : collection.listIndexes())
        {
            Document key = indexDoc.get("key", Document.class);
            if (key == null
                    || key.size() != 1)
            {
                // Compound indexes are discovered but not exposed as seek targets in v1
                continue;
            }

            Entry<String, Object> entry = key.entrySet()
                    .iterator()
                    .next();
            String field = entry.getKey();
            if (MongoCatalog.ID_COLUMN.equals(field))
            {
                // Always declared separately, unconditionally
                continue;
            }
            else if (!(entry.getValue() instanceof Number))
            {
                // text/2dsphere/hashed etc. indexes aren't usable for equality seeks
                continue;
            }

            result.add(new MongoIndex(field));
        }
        return result;
    }

    /** Return collection names for provided database. */
    static List<String> listCollectionNames(MongoClient client, String database)
    {
        List<String> result = new ArrayList<>();
        client.getDatabase(database)
                .listCollectionNames()
                .forEach(result::add);
        return result;
    }

    /** A single-field, seekable Mongo index. */
    record MongoIndex(String column)
    {
    }
}
