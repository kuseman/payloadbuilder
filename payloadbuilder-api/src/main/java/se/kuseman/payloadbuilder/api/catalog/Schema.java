package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.TupleVector;

/**
 * Schema consisting of a list of columns with name and type. Used during query planning and as return schema from {@link TupleVector}'s
 */
public class Schema
{
    public static final Schema EMPTY = new Schema(emptyList());

    private final List<Column> columns;

    public Schema(List<Column> columns)
    {
        this.columns = unmodifiableList(requireNonNull(columns, "columns"));
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public int getSize()
    {
        return columns.size();
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
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
        else if (obj instanceof Schema)
        {
            Schema that = (Schema) obj;
            return columns.equals(that.columns);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return columns.toString();
    }

    /** Construct a schema from provided columns */
    public static Schema of(Column... columns)
    {
        return new Schema(asList(columns));
    }
}
