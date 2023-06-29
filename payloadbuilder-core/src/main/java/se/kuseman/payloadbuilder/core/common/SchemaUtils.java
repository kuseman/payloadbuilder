package se.kuseman.payloadbuilder.core.common;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;

/** Utils when working with {@link Schema}' */
public class SchemaUtils
{
    /** Populate this schema with a populated column. Returns a new schema */
    public static Schema populate(Schema target, String name, Schema populatedSchema)
    {
        List<Column> columns = new ArrayList<>(target.getSize() + 1);
        columns.addAll(target.getColumns());
        ColumnReference colRef = populatedSchema.getSize() > 0 ? getColumnReference(populatedSchema.getColumns()
                .get(0))
                : null;
        colRef = colRef != null ? colRef.rename(name)
                : null;

        Column populatedColumn = CoreColumn.of(name, ResolvedType.table(populatedSchema), colRef);
        columns.add(populatedColumn);
        return new Schema(columns);
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
        List<Column> columns = new ArrayList<>(schema1.getSize() + schema2.getSize());
        columns.addAll(schema1.getColumns());
        columns.addAll(schema2.getColumns());
        return new Schema(columns);
    }

    /** Returns true if this schema contains asterisk columns */
    public static boolean isAsterisk(Schema schema)
    {
        int size = schema.getSize();
        if (size == 0)
        {
            return true;
        }

        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            if (isAsterisk(column))
            {
                return true;
            }
            if (column.getType()
                    .getType() == Column.Type.Table
                    || column.getType()
                            .getType() == Column.Type.Object)
            {
                return isAsterisk(column.getType()
                        .getSchema());
            }
        }
        return false;
    }

    /** Returns true if provided column is asterisk */
    public static boolean isAsterisk(Column column)
    {
        ColumnReference colRef = getColumnReference(column);
        return colRef != null
                && colRef.isAsterisk();
    }

    /** Returns true if provided column is internal */
    public static boolean isInternal(Column column)
    {
        return column instanceof CoreColumn
                && ((CoreColumn) column).isInternal();
    }

    /** Return {@link ColumnReference} from provided column if it'a an instanceof {@link CoreColumn} otherwise null */
    public static ColumnReference getColumnReference(Column column)
    {
        if (!(column instanceof CoreColumn))
        {
            return null;
        }
        return ((CoreColumn) column).getColumnReference();
    }

}
