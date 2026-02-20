package se.kuseman.payloadbuilder.bytes.example;

import static java.util.Collections.singletonMap;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/**
 * Schema for a single sku row. Column ids are assigned once and must never be reused for a different column, even if the column they belonged to is later removed - see
 * {@code PayloadReader#readTupleVector} for the full column-id based schema evolution scheme this relies on.
 */
final class SkuSchema
{
    // Stable forever. Assign the next free number when adding a field, never renumber, never reuse a retired one.
    private static final int ID_ID = 1;
    private static final int NAME_ID = 2;
    private static final int PRICE_ID = 3;

    // Logical ordinals - this schema's own column order. The wrapper layer only ever uses these; id based
    // reconciliation and physical-slot remapping are handled underneath, invisibly to this code.
    static final int ID = 0;
    static final int NAME = 1;
    static final int PRICE = 2;
    static final int COLUMN_COUNT = 3;

    static final Schema SCHEMA = Schema.of(col("id", Column.Type.Int, ID_ID), col("name", Column.Type.String, NAME_ID), col("price", Column.Type.Decimal, PRICE_ID));

    private static Column col(String name, Column.Type type, int id)
    {
        return new Column(name, ResolvedType.of(type), new MetaData(singletonMap(MetaData.COLUMN_ID, id)));
    }

    private SkuSchema()
    {
    }
}
