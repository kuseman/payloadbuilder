package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.physicalplan.ProjectionUtils;

/**
 * Implementation of function object_array. Creates an array of objects from provided arguments.
 */
class ObjectArrayFunctionImpl
{
    static ResolvedType getAggregateType(List<IExpression> arguments)
    {
        // This is the aggregate but the arguments are considered non aggregate for the schema
        Schema schema = ProjectionUtils.createSchema(Schema.EMPTY, arguments, false, false);
        return ResolvedType.table(schema);
    }

    static ResolvedType getOperatorType(Schema input)
    {
        return ResolvedType.table(input);
    }

    /** Operator eval */
    static ValueVector evalOperator(TupleVector input, IExecutionContext context)
    {
        int rowCount = input.getRowCount();
        if (rowCount == 0)
        {
            return ValueVector.literalNull(getOperatorType(input.getSchema()), 1);
        }

        // Tuple vector logically implements Array<Object> so simply return that
        return ValueVector.literalTable(input, 1);
    }

    /** Aggregate eval */
    static ValueVector evalAggregate(ValueVector groups, List<IExpression> expressions, IExecutionContext context)
    {
        int groupSize = groups.size();

        ResolvedType aggregateType = getAggregateType(expressions);
        Schema schema = aggregateType.getSchema();

        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(aggregateType, groupSize);

        int size = expressions.size();

        for (int i = 0; i < groupSize; i++)
        {
            TupleVector group = groups.getTable(i);
            int rowCount = group.getRowCount();
            if (rowCount == 0)
            {
                builder.put(null);
            }
            else
            {
                List<ValueVector> vectors = new ArrayList<>(size);
                for (int j = 0; j < size; j++)
                {
                    vectors.add(expressions.get(j)
                            .eval(group, context));
                }
                builder.put(TupleVector.of(schema, vectors));
            }
        }
        return builder.build();
    }
}
