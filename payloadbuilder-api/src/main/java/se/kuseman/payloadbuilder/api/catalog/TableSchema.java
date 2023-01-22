package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

/** Holder that contains information about a table. Schema, indices etc. */
public class TableSchema
{
    public static final TableSchema EMPTY = new TableSchema();

    private final Schema schema;
    private final List<Index> indices;

    public TableSchema()
    {
        this(Schema.EMPTY, emptyList());
    }

    public TableSchema(Schema schema)
    {
        this(schema, emptyList());
    }

    public TableSchema(Schema schema, List<Index> indices)
    {
        this.schema = requireNonNull(schema, "schema");
        this.indices = requireNonNull(indices, "indices");
    }

    public Schema getSchema()
    {
        return schema;
    }

    public List<Index> getIndices()
    {
        return indices;
    }

    @Override
    public int hashCode()
    {
        return schema.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof TableSchema)
        {
            TableSchema that = (TableSchema) obj;
            return schema.equals(that.schema)
                    && indices.equals(that.indices);
        }
        return false;
    }
}
