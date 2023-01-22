package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.StringUtils;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
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
            ColumnReference colRef = e.getColumnReference();

            // This is an asterisk select for a table in a schema less query
            // Then expand the expressions to match the input tuple vector for that table source
            if (colRef != null
                    && colRef.isAsterisk())
            {
                boolean aggregate = e instanceof IAggregateExpression;

                TableSourceReference tableSource = colRef.getTableSource();

                if (!findColumns(schema, false, tableSource, result, aggregate)
                        && outerSchema != null)
                {
                    findColumns(outerSchema, true, tableSource, result, aggregate);
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

    private static boolean findColumns(Schema schema, boolean outer, TableSourceReference tableSource, List<IExpression> result, boolean aggregate)
    {
        boolean added = false;
        int size = schema.getColumns()
                .size();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);
            ColumnReference columnColRef = column.getColumnReference();
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
                        .getType() == Type.TupleVector)
                {
                    Schema innerSchema = column.getType()
                            .getSchema();
                    int innerSize = innerSchema.getSize();
                    for (int j = 0; j < innerSize; j++)
                    {
                        Column innerColumn = innerSchema.getColumns()
                                .get(j);
                        result.add(new DereferenceExpression(columnExpression, innerColumn.getName(), j, ResolvedType.valueVector(innerColumn.getType())));
                    }
                }
                else
                {
                    if (aggregate)
                    {
                        columnExpression = new AggregateWrapperExpression(columnExpression, true, false);
                    }
                    result.add(columnExpression);
                }
                added = true;
            }
        }
        return added;
    }

    /** Create a schema from provided expressions */
    public static Schema createSchema(Schema input, List<? extends IExpression> expressions, boolean appendInputColumns)
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

            ResolvedType type = expression.getType();
            ColumnReference columnReference = expression.getColumnReference();
            columns.add(new Column(name, outputName, type, columnReference, expression.isInternal()));
        }

        if (appendInputColumns)
        {
            columns.addAll(input.getColumns());
        }

        return new Schema(columns);
    }
}
