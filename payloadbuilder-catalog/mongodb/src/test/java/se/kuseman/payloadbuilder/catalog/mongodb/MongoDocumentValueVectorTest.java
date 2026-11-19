package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Unit tests of {@link MongoDocumentValueVector}. */
class MongoDocumentValueVectorTest
{
    @Test
    void test_type_is_always_any()
    {
        MongoDocumentValueVector vector = new MongoDocumentValueVector(List.of(new Document()), "field");
        assertEquals(ResolvedType.of(Type.Any), vector.type());
    }

    @Test
    void test_size_matches_document_count()
    {
        MongoDocumentValueVector vector = new MongoDocumentValueVector(List.of(new Document(), new Document()), "field");
        assertEquals(2, vector.size());
    }

    @Test
    void test_missing_field_is_null()
    {
        MongoDocumentValueVector vector = new MongoDocumentValueVector(List.of(new Document()), "missing");
        assertTrue(vector.isNull(0));
    }

    @Test
    void test_objectId_is_converted_to_hex_string()
    {
        ObjectId id = new ObjectId();
        Document doc = new Document("_id", id);
        MongoDocumentValueVector vector = new MongoDocumentValueVector(List.of(doc), "_id");
        assertEquals(id.toHexString(), vector.getAny(0));
    }

    @Test
    void test_conversion_is_lazy_and_cached_per_row()
    {
        CountingDocument doc0 = new CountingDocument("field", "value0");
        CountingDocument doc1 = new CountingDocument("field", "value1");
        MongoDocumentValueVector vector = new MongoDocumentValueVector(List.of(doc0, doc1), "field");

        // Nothing accessed yet - no document should have been touched
        assertEquals(0, doc0.getCount.get());
        assertEquals(0, doc1.getCount.get());

        assertEquals("value0", vector.getAny(0));
        assertEquals(1, doc0.getCount.get());
        // Row 1 was never accessed - its (hypothetically expensive) conversion never ran
        assertEquals(0, doc1.getCount.get());

        // Second access to the same row is served from the cache, not re-read from the document
        vector.getAny(0);
        assertEquals(1, doc0.getCount.get());

        assertEquals("value1", vector.getAny(1));
        assertEquals(1, doc1.getCount.get());
    }

    private static final class CountingDocument extends Document
    {
        final AtomicInteger getCount = new AtomicInteger();

        CountingDocument(String key, Object value)
        {
            super(key, value);
        }

        @Override
        public Object get(Object key)
        {
            getCount.incrementAndGet();
            return super.get(key);
        }
    }
}
