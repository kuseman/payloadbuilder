package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression.Builder;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

/** Utility class for projections that handles asterisks in projection expressions. */
public class ProjectionUtils
{
    /** Expands expressions regarding asterisk. NOTE! Should not be called if no asterisks exists in expressions */
    public static List<IExpression> expandExpressions(List<? extends IExpression> expressions, Schema outerSchema, Schema schema)
    {
        List<IExpression> result = new ArrayList<>(expressions.size() + schema.getColumns()
                .size());

        for (IExpression e : expressions)
        {
            boolean aggregateSingleValue = false;
            boolean aggregate = e instanceof IAggregateExpression;
            if (e instanceof AggregateWrapperExpression awe)
            {
                aggregateSingleValue = awe.isSingleValue();

                if (!(awe.getExpression() instanceof AsteriskExpression))
                {
                    result.add(e);
                    continue;
                }

                e = awe.getExpression();
            }

            if (!(e instanceof AsteriskExpression))
            {
                result.add(e);
                continue;
            }
            AsteriskExpression ae = (AsteriskExpression) e;
            // Non qualified asterisk, add all columns from input as expressions
            // NOTE! A non qualified asterisk cannot be present in an outer context so we only use input schema here.
            if (ae.getQname()
                    .size() == 0)
            {
                findColumns(schema, false, null, result, aggregate, aggregateSingleValue, ae.getQname());
                continue;
            }

            for (TableSourceReference tableRef : ae.getTableSourceReferences())
            {
                if (!findColumns(schema, false, tableRef, result, aggregate, aggregateSingleValue, ae.getQname())
                        && outerSchema != null)
                {
                    findColumns(outerSchema, true, tableRef, result, aggregate, aggregateSingleValue, ae.getQname());
                }
            }
        }

        if (result.isEmpty())
        {
            throw new IllegalArgumentException("No expressions could be expanded from " + schema);
        }

        return result;
    }

    private static boolean findColumns(Schema schema, boolean outer, TableSourceReference tableSource, List<IExpression> result, boolean aggregate, boolean aggregateSingleValue,
            QualifiedName asteriskQname)
    {
        boolean added = false;
        int size = schema.getColumns()
                .size();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            // Internal columns should not be expanded
            if (SchemaUtils.isInternal(column))
            {
                continue;
            }

            TableSourceReference columnTableSource = SchemaUtils.getTableSource(column);
            // Matching table source or add all, return expression
            if (tableSource == null
                    || (columnTableSource != null
                            && tableSource.getId() == columnTableSource.getId()))
            {
                CoreColumn.Type columnType = SchemaUtils.getColumnType(column);

                Builder builder = ColumnExpression.Builder.of(column.getName(), column.getType())
                        .withOuterReference(outer)
                        .withOrdinal(i);

                if (columnTableSource != null)
                {
                    builder.withColumnReference(new ColumnReference(column.getName(), columnTableSource, columnType));
                }

                IExpression columnExpression = builder.build();

                // Populated column, add a dereference for all of the inner schema columns
                // We only do this when we target an alias
                if (SchemaUtils.isPopulated(column)
                        && asteriskQname.size() > 0)
                {
                    Schema innerSchema = column.getType()
                            .getSchema();
                    int innerSize = innerSchema.getSize();
                    for (int j = 0; j < innerSize; j++)
                    {
                        Column innerColumn = innerSchema.getColumns()
                                .get(j);

                        IExpression dereference = new DereferenceExpression(columnExpression, innerColumn.getName(), j, ResolvedType.array(innerColumn.getType()));
                        if (aggregate)
                        {
                            dereference = new AggregateWrapperExpression(dereference, aggregateSingleValue, false);
                        }
                        result.add(dereference);
                    }
                }
                else
                {
                    if (aggregate)
                    {
                        columnExpression = new AggregateWrapperExpression(columnExpression, aggregateSingleValue, false);
                    }
                    result.add(columnExpression);
                }
                added = true;
            }
        }
        return added;
    }
}
