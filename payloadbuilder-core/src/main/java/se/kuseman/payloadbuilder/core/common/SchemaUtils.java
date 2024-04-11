package se.kuseman.payloadbuilder.core.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasAlias.Alias;
import se.kuseman.payloadbuilder.core.expression.HasTableSourceReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

/** Utils when working with {@link Schema}' */
public class SchemaUtils
{
    /** Populate this schema with a populated column. Returns a new schema */
    public static Schema populate(Schema target, String name, Schema populatedSchema)
    {
        List<Column> columns = new ArrayList<>(target.getSize() + 1);
        columns.addAll(target.getColumns());
        columns.add(getPopulatedColumn(name, populatedSchema));
        return new Schema(columns);
    }

    /** Create a populated column with provided name and schema */
    public static Column getPopulatedColumn(String name, Schema populatedSchema)
    {
        TableSourceReference tableRef = getTableSource(populatedSchema);
        return new CoreColumn(name, ResolvedType.table(populatedSchema), "", false, tableRef, CoreColumn.Type.POPULATED);
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

        List<Column> columns1 = schema1.getColumns();
        List<Column> columns2 = schema2.getColumns();

        int size1 = columns1.size();
        int size = size1 + columns2.size();
        List<Column> columns = new AbstractList<>()
        {
            @Override
            public int size()
            {
                return size;
            }

            @Override
            public Column get(int index)
            {
                if (index < size1)
                {
                    return columns1.get(index);
                }
                return columns2.get(index - size1);
            }
        };
        return new Schema(columns);
    }

    /** Returns true if this schema contains asterisk columns */
    public static boolean isAsterisk(Schema schema)
    {
        return isAsterisk(schema, false);
    }

    /** Returns true if this schema contains asterisk columns */
    public static boolean isAsterisk(Schema schema, boolean currentLevelOnly)
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

            if (!currentLevelOnly
                    && (column.getType()
                            .getType() == Column.Type.Table
                            || column.getType()
                                    .getType() == Column.Type.Object))
            {
                if (isAsterisk(column.getType()
                        .getSchema()))
                {
                    return true;
                }
            }
        }
        return false;
    }

    /** Returns true if provided column is asterisk */
    public static boolean isAsterisk(Column column)
    {
        return column instanceof CoreColumn cc
                && cc.getColumnType() == CoreColumn.Type.ASTERISK;
    }

    /** Returns true if provided column is populated */
    public static boolean isPopulated(Column column)
    {
        return column instanceof CoreColumn cc
                && cc.getColumnType() == CoreColumn.Type.POPULATED;
    }

    /** Returns true if provided column is internal */
    public static boolean isInternal(Column column)
    {
        return column instanceof CoreColumn cc
                && cc.isInternal();
    }

    /** Return {@link TableSourceReference} from provided column if it'a an instanceof {@link CoreColumn} otherwise null */
    public static TableSourceReference getTableSource(Column column)
    {
        if (column instanceof CoreColumn cc)
        {
            return cc.getTableSourceReference();
        }
        return null;
    }

    /** Return table source reference from provided schema. If multiple table sources found null is returned. */
    public static TableSourceReference getTableSource(Schema schema)
    {
        Set<Integer> seenTableSources = new HashSet<>();
        TableSourceReference result = null;
        for (Column column : schema.getColumns())
        {
            TableSourceReference tableSource = getTableSource(column);
            // No table source or not one and the same on whole schema => no table source
            if (tableSource == null)
            {
                return null;
            }

            seenTableSources.add(tableSource.getId());
            if (seenTableSources.size() > 1)
            {
                return null;
            }

            result = tableSource;
        }
        return result;
    }

    /** Create a schema from provided expressions. */
    public static Schema getSchema(Schema schema, List<? extends IExpression> expressions, boolean appendInputColumns, boolean aggregate)
    {
        List<Column> columns = new ArrayList<>(expressions.size() + (appendInputColumns ? schema.getSize()
                : 0));

        for (IExpression expression : expressions)
        {
            String name = "";
            String outputName = "";
            if (expression instanceof HasAlias a)
            {
                Alias alias = a.getAlias();
                name = alias.getAlias();
                outputName = alias.getOutputAlias();
            }

            if (StringUtils.isBlank(name))
            {
                outputName = expression.toString();
            }

            ResolvedType type = aggregate ? ((IAggregateExpression) expression).getAggregateType()
                    : expression.getType();
            TableSourceReference tableSourceReference = null;
            boolean asterisk = expression instanceof AsteriskExpression
                    || expression instanceof AggregateWrapperExpression awe
                            && awe.getExpression() instanceof AsteriskExpression;
            if (expression instanceof HasTableSourceReference htsr)
            {
                tableSourceReference = htsr.getTableSourceReference();
            }
            columns.add(new CoreColumn(name, type, outputName, expression.isInternal(), tableSourceReference, asterisk ? CoreColumn.Type.ASTERISK
                    : CoreColumn.Type.REGULAR));
        }

        if (appendInputColumns)
        {
            columns.addAll(schema.getColumns());
        }

        return new Schema(columns);
    }
}
