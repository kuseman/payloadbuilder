package se.kuseman.payloadbuilder.core.common;

import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasAlias.Alias;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

/** Utils when working with {@link Schema}' */
public class SchemaUtils
{
    /** Create a join schema from two provided schemas */
    public static Schema joinSchema(Schema outer, Schema inner)
    {
        return concat(outer, inner);
    }

    /** Create a join schema from two provided schemas */
    public static Schema joinSchema(Schema outer, Schema inner, String populateAlias)
    {
        if (populateAlias == null)
        {
            return concat(outer, inner);
        }
        return populate(outer, populateAlias, inner);
    }

    /** Populate this schema with a populated column. Returns a new schema */
    private static Schema populate(Schema target, String name, Schema populatedSchema)
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

    /** Creates a new column from provide column with a new name. */
    public static Column rename(Column column, String newName)
    {
        if (column instanceof CoreColumn cc)
        {
            return new CoreColumn(cc, newName);
        }

        return new Column(newName, column.getType());
    }

    /** Creates a new column from provide column with a new type. */
    public static Column changeTableSource(Column column, TableSourceReference tableSourceReference)
    {
        if (column instanceof CoreColumn cc)
        {
            return new CoreColumn(cc, cc.getType(), tableSourceReference);
        }

        return CoreColumn.of(column.getName(), column.getType(), tableSourceReference);
    }

    /** Creates a new column from provide column with a new type. */
    public static Column changeType(Column column, ResolvedType type)
    {
        return changeType(column, type, getTableSource(column));
    }

    /** Creates a new column from provide column with a new type. */
    public static Column changeType(Column column, ResolvedType type, TableSourceReference tableSourceReference)
    {
        if (column instanceof CoreColumn cc)
        {
            return new CoreColumn(cc, type, tableSourceReference);
        }

        return CoreColumn.of(column.getName(), type, tableSourceReference);
    }

    /** Return a new schema which concats to other schemas */
    private static Schema concat(Schema schema1, Schema schema2)
    {
        if (schema1 == null
                || schema1.getSize() == 0)
        {
            return schema2;
        }
        else if (schema2 == null
                || schema2.getSize() == 0)
        {
            return schema1;
        }

        List<Column> columns1 = schema1.getColumns();
        List<Column> columns2 = schema2.getColumns();
        List<Column> columns = new ArrayList<>(columns1.size() + columns2.size());
        columns.addAll(columns1);
        columns.addAll(columns2);
        return new Schema(columns);
    }

    /** Returns true if this schema contains asterisk columns */
    public static boolean isAsterisk(Schema schema)
    {
        if (schema == null)
        {
            return false;
        }
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
        }
        return false;
    }

    /**
     * Returns true if provided column is asterisk.
     */
    public static boolean isAsterisk(Column column)
    {
        if (column instanceof CoreColumn cc)
        {
            if (cc.getColumnType() == CoreColumn.Type.ASTERISK)
            {
                return true;
            }

            // A populated column who's schema is asterisk counts as asterisk
            if (cc.getColumnType() == CoreColumn.Type.POPULATED)
            {
                return isAsterisk(cc.getType()
                        .getSchema());
            }

            // If we have an internal column who's type is a table that is an asterisk schema
            // this column is treated as asterisk
            if (cc.isInternal()
                    && cc.getType()
                            .getType() == Column.Type.Table)
            {
                return isAsterisk(cc.getType()
                        .getSchema());
            }

        }
        return false;
    }

    /** Return column type of provided column */
    public static CoreColumn.Type getColumnType(Column column)
    {
        if (column instanceof CoreColumn cc)
        {
            return cc.getColumnType();
        }
        return CoreColumn.Type.REGULAR;
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

    /** Collect all table sources referenced by provided column. */
    public static Set<TableSourceReference> getTableSources(Column column)
    {
        if (column instanceof CoreColumn cc)
        {
            Set<TableSourceReference> result = new HashSet<>();
            List<Column> queue = new ArrayList<>();
            queue.add(cc);
            while (queue.size() > 0)
            {
                Column c = queue.remove(0);
                TableSourceReference tr = getTableSource(c);
                if (tr != null)
                {
                    result.add(tr);
                }
                if (c.getType()
                        .getSchema() != null)
                {
                    queue.addAll(c.getType()
                            .getSchema()
                            .getColumns());
                }
            }

            return result;
        }
        return emptySet();
    }

    /**
     * Create a schema from provided expressions.
     *
     * @param parentTableSource Reference to mark expressions that lack a table source with.
     */
    public static Schema getSchema(TableSourceReference parentTableSource, List<? extends IExpression> expressions, boolean aggregate)
    {
        List<Column> columns = new ArrayList<>(expressions.size());
        for (IExpression expression : expressions)
        {
            columns.add(getColumn(parentTableSource, expression, aggregate, null));
        }
        return new Schema(columns);
    }

    /**
     * Create a schema from provided expression and runtime vectors. This is used during schema less queries to create a schema based on runtime values
     */
    public static Schema getSchema(TableSourceReference parentTableSource, List<? extends IExpression> expressions, ValueVector[] projectionVectors, boolean aggregate)
    {
        // Else we need to construct the actual schema from the actual vectors
        List<Column> columns = new ArrayList<>(projectionVectors.length);

        // First add projection types
        int size = projectionVectors.length;
        for (int i = 0; i < size; i++)
        {
            columns.add(getColumn(parentTableSource, expressions.get(i), aggregate, projectionVectors[i].type()));
        }
        return new Schema(columns);
    }

    /** Create a column from provided expression and optional type */
    private static Column getColumn(TableSourceReference parentTableSource, IExpression expression, boolean aggregate, ResolvedType type)
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

        // No type provided then use the type from the expression
        if (type == null)
        {
            type = aggregate ? ((IAggregateExpression) expression).getAggregateType()
                    : expression.getType();
        }
        TableSourceReference tableSourceReference = null;
        CoreColumn.Type columnType = isAsteriskExpression(expression) ? CoreColumn.Type.ASTERISK
                : CoreColumn.Type.REGULAR;
        if (expression instanceof HasColumnReference hcr)
        {
            ColumnReference cr = hcr.getColumnReference();
            if (cr != null)
            {
                tableSourceReference = cr.tableSourceReference();
                columnType = cr.columnType();
            }
        }
        if (tableSourceReference == null)
        {
            tableSourceReference = parentTableSource;
        }
        return new CoreColumn(name, type, outputName, expression.isInternal(), tableSourceReference, columnType);
    }

    private static boolean isAsteriskExpression(IExpression expression)
    {
        return expression instanceof AsteriskExpression
                || expression instanceof AggregateWrapperExpression awe
                        && awe.getExpression() instanceof AsteriskExpression;
    }
}
