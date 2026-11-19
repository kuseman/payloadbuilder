package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Projections;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;

/** Unit tests of {@link MongoProjectionBuilder}. */
class MongoProjectionBuilderTest
{
    @Test
    void test_columns_projection_is_pushed_down()
    {
        Bson projection = MongoProjectionBuilder.build(Projection.columns(of("a", "b")));
        assertEquals(toJson(Projections.include("a", "b")), toJson(projection));
    }

    @Test
    void test_all_projection_is_not_restricted()
    {
        assertNull(MongoProjectionBuilder.build(Projection.ALL));
    }

    @Test
    void test_none_projection_is_not_restricted()
    {
        assertNull(MongoProjectionBuilder.build(Projection.NONE));
    }

    private static String toJson(Bson bson)
    {
        return bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson();
    }
}
