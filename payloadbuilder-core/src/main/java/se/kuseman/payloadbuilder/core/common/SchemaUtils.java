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
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn.Type;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasAlias.Alias;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

/** Utils when working with {@link Schema}' */
public final class SchemaUtils
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
        ColumnReference cr = null;
        if (tableRef != null)
        {
            cr = new ColumnReference(name, tableRef, Column.MetaData.EMPTY);
        }

        return CoreColumn.Builder.from(name, ResolvedType.table(populatedSchema))
                .withColumnType(CoreColumn.Type.POPULATED)
                .withColumnReference(cr)
                .build();
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

    /**
     * Returns true if provided schema has columns that originates from any asterisk input. Ie. any column on any level that is of type {@link CoreColumn.Type#ASTERISK} or
     * {@link CoreColumn.Type#NAMED_ASTERISK}
     */
    public static boolean originatesFromAsteriskInput(Schema schema)
    {
        return isAsterisk(schema, true, true);
    }

    /** Returns true if this schema contains asterisk columns. One or more columns of type {@link CoreColumn.Type#ASTERISK}. */
    public static boolean isAsterisk(Schema schema)
    {
        return isAsterisk(schema, false, false);
    }

    private static boolean isAsterisk(Schema schema, boolean includeNamedAsterisks, boolean traverseIntoSubSchemas)
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
            if (isAsterisk(column, includeNamedAsterisks, traverseIntoSubSchemas))
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
        return isAsterisk(column, false, false);
    }

    private static boolean isAsterisk(Column column, boolean includeNamedAsterisks, boolean traverseIntoSubSchemas)
    {
        if (column instanceof CoreColumn cc)
        {
            if (cc.getColumnType() == CoreColumn.Type.ASTERISK
                    || (includeNamedAsterisks
                            && cc.getColumnType() == Type.NAMED_ASTERISK))
            {
                return true;
            }

            // Dig down into complex type schema
            // We also do this for populated columns as those count as asterisk
            // if their column is asterisk
            if ((traverseIntoSubSchemas
                    || cc.getColumnType() == CoreColumn.Type.POPULATED)
                    && cc.getType()
                            .getSchema() != null)
            {
                return isAsterisk(cc.getType()
                        .getSchema(), includeNamedAsterisks, traverseIntoSubSchemas);
            }
        }
        return false;
    }

    /** Return column type of provided column */
    public static CoreColumn.Type getColumnType(Column column)
    {
        return column instanceof CoreColumn cc ? cc.getColumnType()
                : CoreColumn.Type.REGULAR;
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
        return column instanceof CoreColumn cc ? cc.getTableSourceReference()
                : null;
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

    /** Re-writes the input schema and connects runtime schema data in schema less queries. */
    public static Schema rewriteSchema(Schema schema, StatementContext statementContext)
    {
        return new Schema(schema.getColumns()
                .stream()
                .map(c -> rewriteColumn(c, statementContext))
                .toList());
    }

    private static Column rewriteColumn(Column column, StatementContext statementContexnt)
    {
        ResolvedType type = column.getType();
        if (column.getType()
                .getSchema() != null)
        {
            Schema schema = rewriteSchema(column.getType()
                    .getSchema(), statementContexnt);
            type = column.getType()
                    .getType() == Column.Type.Object ? ResolvedType.object(schema)
                            : ResolvedType.table(schema);
        }
        else if (column.getType()
                .getSubType() != null
                && column.getType()
                        .getSubType()
                        .getSchema() != null)
        {
            Schema schema = rewriteSchema(column.getType()
                    .getSubType()
                    .getSchema(), statementContexnt);
            ResolvedType subType = column.getType()
                    .getSubType()
                    .getType() == Column.Type.Object ? ResolvedType.object(schema)
                            : ResolvedType.table(schema);
            type = ResolvedType.array(subType);
        }

        // Fetch the meta data from the runtime data
        ColumnReference cr = getColumnReference(column);
        Column.MetaData metaData = Column.MetaData.EMPTY;
        if (cr != null)
        {
            Schema runtimeSchema = statementContexnt.getRuntimeSchema(cr.tableSourceReference()
                    .getRoot()
                    .getId());
            if (runtimeSchema != null)
            {
                Column runtimeColumn = runtimeSchema.getColumns()
                        .stream()
                        .filter(c -> c.getName()
                                .equalsIgnoreCase(cr.columnName()))
                        .findFirst()
                        .orElse(null);
                metaData = runtimeColumn != null ? runtimeColumn.getMetaData()
                        : Column.MetaData.EMPTY;
            }
        }

        return CoreColumn.Builder.from(column)
                .withResolvedType(type)
                .withMetaData(metaData)
                .build();
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
        CoreColumn.Type columnType = getColumnType(expression);
        ColumnReference cr = getColumnReference(expression);
        if (cr == null
                && parentTableSource != null)
        {
            cr = new ColumnReference(columnType == Type.ASTERISK ? "*"
                    : name, parentTableSource, Column.MetaData.EMPTY);
        }
        return CoreColumn.Builder.from(name, type)
                .withOutputName(outputName)
                .withInternal(expression.isInternal())
                .withColumnReference(cr)
                .withColumnType(columnType)
                .build();
    }

    private static ColumnReference getColumnReference(Column column)
    {
        return column instanceof CoreColumn cc ? cc.getColumnReference()
                : null;
    }

    /** Extract a {@link ColumnExpression} from provided expression. */
    public static ColumnReference getColumnReference(IExpression e)
    {
        // Unwrap nested expressions
        if (e instanceof AggregateWrapperExpression awe)
        {
            e = awe.getExpression();
        }
        if (e instanceof AliasExpression ae)
        {
            e = ae.getExpression();
        }

        if (e instanceof AsteriskExpression ae
                && ae.getTableSourceReferences()
                        .size() == 1)
        {
            return new ColumnReference("*", ae.getTableSourceReferences()
                    .iterator()
                    .next(), Column.MetaData.EMPTY);
        }
        else if (e instanceof ColumnExpression ce)
        {
            return ce.getColumnReference();
        }
        // Deref on a populated column expression => return the column expression reference but with the de-refs right part
        else if (e instanceof DereferenceExpression de
                && de.getExpression() instanceof ColumnExpression ce
                && ce.getColumnType() == CoreColumn.Type.POPULATED
                && ce.getColumnReference() != null)
        {
            return new ColumnReference(de.getRight(), ce.getColumnReference()
                    .tableSourceReference(),
                    ce.getColumnReference()
                            .metaData());
        }

        return null;
    }

    // CSOFF
    public static CoreColumn.Type getColumnType(IExpression e)
    // CSON
    {
        // Unwrap nested expressions
        if (e instanceof AggregateWrapperExpression awe)
        {
            e = awe.getExpression();
        }
        if (e instanceof AliasExpression ae)
        {
            e = ae.getExpression();
        }

        if (e instanceof AsteriskExpression)
        {
            return CoreColumn.Type.ASTERISK;
        }
        else if (e instanceof ColumnExpression ce)
        {
            return ce.getColumnType();
        }

        return CoreColumn.Type.REGULAR;
    }
}
