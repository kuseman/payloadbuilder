package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
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
            ColumnReference colRef = null;
            if (e instanceof HasColumnReference)
            {
                colRef = ((HasColumnReference) e).getColumnReference();
            }

            // This is an asterisk select for a table in a schema less query
            // Then expand the expressions to match the input tuple vector for that table source
            if (colRef != null
                    && colRef.isAsterisk())
            {
                boolean aggregate = e instanceof IAggregateExpression;
                boolean aggregateSingleValue = e instanceof AggregateWrapperExpression ? ((AggregateWrapperExpression) e).isSingleValue()
                        : false;

                TableSourceReference tableSource = colRef.getTableSource();

                if (!findColumns(schema, false, tableSource, result, aggregate, aggregateSingleValue)
                        && outerSchema != null)
                {
                    findColumns(outerSchema, true, tableSource, result, aggregate, aggregateSingleValue);
                }
            }
            else
            {
                result.add(e);
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
            ColumnReference columnColRef = SchemaUtils.getColumnReference(column);
            TableSourceReference columnTableSource = columnColRef != null ? columnColRef.getTableSource()
                    : null;

            // Matching table source, return expression
            if (tableSource.equals(columnTableSource))
            {
                IExpression columnExpression = ColumnExpression.Builder.of(columnColRef.getName(), column.getType())
                        .withColumnReference(columnColRef)
                        .withOuterReference(outer)
                        .withOrdinal(i)
                        .build();

                // Populated column, add a dereference for all of the inner schema columns
                if (column.getType()
                        .getType() == Type.Table)
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

    /** Create a schema from provided expressions */
    public static Schema createSchema(Schema input, List<? extends IExpression> expressions, boolean appendInputColumns, boolean aggregate)
    {
        int inputSize = input.getSize();
        List<Column> columns = new ArrayList<>(expressions.size() + (appendInputColumns ? inputSize
                : 0));
        for (IExpression expression : expressions)
        {
            String name = "";
            String outputName = "";
            if (expression instanceof HasAlias)
            {
                HasAlias.Alias alias = ((HasAlias) expression).getAlias();
                name = alias.getAlias();
                outputName = alias.getOutputAlias();
            }

            if (StringUtils.isBlank(name))
            {
                outputName = expression.toString();
            }

            ResolvedType type;
            if (aggregate)
            {
                type = ((IAggregateExpression) expression).getAggregateType();
            }
            else
            {
                type = expression.getType();
            }
            ColumnReference columnReference = null;
            if (expression instanceof HasColumnReference)
            {
                columnReference = ((HasColumnReference) expression).getColumnReference();
            }
            columns.add(CoreColumn.of(name, type, outputName, expression.isInternal(), columnReference));
        }

        if (appendInputColumns)
        {
            columns.addAll(input.getColumns());
        }

        return new Schema(columns);
    }
}
