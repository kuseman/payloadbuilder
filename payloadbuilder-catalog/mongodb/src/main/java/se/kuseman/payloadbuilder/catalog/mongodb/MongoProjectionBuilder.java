package se.kuseman.payloadbuilder.catalog.mongodb;

import org.bson.conversions.Bson;

import com.mongodb.client.model.Projections;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.ProjectionType;

/** Translates a PLB {@link Projection} into a MongoDB projection document. */
final class MongoProjectionBuilder
{
    private MongoProjectionBuilder()
    {
    }

    /** Returns the projection document to push down, or null if no restriction should be applied (wanted projection is ALL/NONE). */
    static Bson build(Projection projection)
    {
        if (projection.type() != ProjectionType.COLUMNS)
        {
            return null;
        }
        return Projections.include(projection.columns());
    }
}
