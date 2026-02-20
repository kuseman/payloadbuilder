package se.kuseman.payloadbuilder.bytes.example;

import static java.util.Collections.singletonMap;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Schema for a single product row, with a nested table of skus. See {@link SkuSchema} for the column-id discipline this follows. */
final class ProductSchema
{
    // Stable forever. Assign the next free number when adding a field, never renumber, never reuse a retired one.
    private static final int ID_ID = 1;
    private static final int NAME_ID = 2;
    private static final int PRICE_ID = 3;
    private static final int SKUS_ID = 4;

    // Logical ordinals - this schema's own column order.
    static final int ID = 0;
    static final int NAME = 1;
    static final int PRICE = 2;
    static final int SKUS = 3;
    static final int COLUMN_COUNT = 4;

    static final Schema SCHEMA = Schema.of(col("id", Column.Type.Int, ID_ID), col("name", Column.Type.String, NAME_ID), col("price", Column.Type.Decimal, PRICE_ID),
            col("skus", ResolvedType.table(SkuSchema.SCHEMA), SKUS_ID));

    private static Column col(String name, Column.Type type, int id)
    {
        return new Column(name, ResolvedType.of(type), new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    private static Column col(String name, ResolvedType type, int id)
    {
        return new Column(name, type, new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    private ProductSchema()
    {
    }
}
