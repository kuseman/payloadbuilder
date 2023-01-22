package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/**
 * The result schema of a plan component. This is the runtime schema returned from {@link TupleVector}'s
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

    /** Return column with provided name or null if not found */
    public Column getColumn(String name)
    {
        int size = getSize();
        for (int i = 0; i < size; i++)
        {
            Column column = columns.get(i);

            if (column.getName()
                    .equalsIgnoreCase(name))
            {
                return column;
            }
        }
        return null;
    }

    /** Returns true if this schema contains asterisk columns */
    public boolean isAsterisk()
    {
        int size = getSize();
        for (int i = 0; i < size; i++)
        {
            Column column = columns.get(i);
            ColumnReference colRef = column.getColumnReference();
            if (colRef != null
                    && colRef.getType() == ColumnReference.Type.ASTERISK)
            {
                return true;
            }
            if (column.getType()
                    .getType() == Type.TupleVector)
            {
                return column.getType()
                        .getSchema()
                        .isAsterisk();
            }
        }
        return false;
    }

    /** Populate this schema with a populated column. Returns a new schema */
    public Schema populate(String name, Schema populatedSchema)
    {
        List<Column> columns = new ArrayList<>(this.columns);
        ColumnReference colRef = populatedSchema.getColumns()
                .get(0)
                .getColumnReference();
        colRef = colRef != null ? colRef.rename(name)
                : null;

        Column populatedColumn = new Column(name, ResolvedType.tupleVector(populatedSchema), colRef);

        columns.add(populatedColumn);
        return new Schema(columns);
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
        if (obj instanceof Schema)
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

    /** Return a new schema which concats to other schemas */
    public static Schema concat(Schema schema1, Schema schema2)
    {
        if (schema1 == null)
        {
            return schema2;
        }
        else if (schema2 == null)
        {
            return schema1;
        }
        List<Column> columns = new ArrayList<>(schema1.columns.size() + schema2.columns.size());
        columns.addAll(schema1.columns);
        columns.addAll(schema2.columns);
        return new Schema(columns);
    }
}
