package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
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
            boolean aggregate = e instanceof IAggregateExpression;
            boolean aggregateSingleValue = false;

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
            for (TableSourceReference tableRef : ae.getTableSourceReferences())
            {
                if (!findColumns(schema, false, tableRef, result, aggregate, aggregateSingleValue)
                        && outerSchema != null)
                {
                    findColumns(outerSchema, true, tableRef, result, aggregate, aggregateSingleValue);
                }
            }
        }

        if (result.isEmpty())
        {
            throw new IllegalArgumentException("No expressions could be expanded from " + schema);
        }

        return result;
    }

    private static boolean findColumns(Schema schema, boolean outer, TableSourceReference tableSource, List<IExpression> result, boolean aggregate, boolean aggregateSingleValue)
    {
        boolean added = false;
        int size = schema.getColumns()
                .size();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            TableSourceReference columnTableSource = SchemaUtils.getTableSource(column);
            if (columnTableSource == null)
            {
                continue;
            }

            // Matching table source, return expression
            if (tableSource.getId() == columnTableSource.getId())
            {
                CoreColumn.Type columnType = SchemaUtils.getColumnType(column);

                IExpression columnExpression = ColumnExpression.Builder.of(column.getName(), column.getType())
                        .withColumnReference(new ColumnReference(columnTableSource, columnType))
                        .withOuterReference(outer)
                        .withOrdinal(i)
                        .build();

                // Populated column, add a dereference for all of the inner schema columns
                if (SchemaUtils.isPopulated(column))
                {
                    Schema innerSchema = column.getType()
                            .getSchema();
                    int innerSize = innerSchema.getSize();
                    for (int j = 0; j < innerSize; j++)
                    {
                        Column innerColumn = innerSchema.getColumns()
                                .get(j);
                        result.add(new DereferenceExpression(columnExpression, innerColumn.getName(), j, ResolvedType.array(innerColumn.getType())));
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
