package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/**
 * Implementation of function array. Creates an array of provided arguments.
 */
class ArrayFunctionImpl
{
    static ResolvedType getScalarType(List<IExpression> arguments)
    {
        if (arguments.isEmpty())
        {
            throw new IllegalArgumentException("arguments is empty");
        }

        ResolvedType type = null;
        for (IExpression arg : arguments)
        {
            if (type == null)
            {
                type = arg.getType();
            }
            else if (!type.equals(arg.getType()))
            {
                type = ResolvedType.of(Type.Any);
                break;
            }
        }

        return ResolvedType.array(type);
    }

    static ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return getScalarType(arguments);
    }

    static ResolvedType getOperatorType(Schema input)
    {
        ResolvedType type = null;
        for (Column col : input.getColumns())
        {
            if (type == null)
            {
                type = col.getType();
            }
            else if (!type.equals(col.getType()))
            {
                type = ResolvedType.of(Type.Any);
                break;
            }
        }

        return ResolvedType.array(type);
    }

    /** Operator eval */
    static ValueVector evalOperator(TupleVector input, IExecutionContext context)
    {
        ResolvedType type = getOperatorType(input.getSchema());
        int rowCount = input.getRowCount();
        if (rowCount == 0)
        {
            return ValueVector.literalNull(type, 0);
        }
        int size = input.getSchema()
                .getSize();
        ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            Column column = input.getSchema()
                    .getColumns()
                    .get(i);
            if (SchemaUtils.isInternal(column))
            {
                continue;
            }

            vectors[i] = input.getColumn(i);
        }

        IValueVectorBuilder arrayBuilder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(type.getSubType(), rowCount * size);

        for (int k = 0; k < rowCount; k++)
        {
            for (int j = 0; j < size; j++)
            {
                if (vectors[j] == null)
                {
                    continue;
                }

                arrayBuilder.put(vectors[j], k);
            }
        }

        return ValueVector.literalArray(arrayBuilder.build(), 1);
    }

    /** Aggregate eval */
    static ValueVector evalAggregate(ValueVector groups, List<IExpression> expressions, IExecutionContext context)
    {
        int groupSize = groups.size();

        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(ResolvedType.array(Type.Any), groupSize);

        int size = expressions.size();
        ValueVector[] valueVectors = new ValueVector[size];

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
                for (int j = 0; j < size; j++)
                {
                    valueVectors[j] = expressions.get(j)
                            .eval(group, context);
                }

                // Only one argument then we can simply add the expression result
                // since that is our array
                if (size == 1)
                {
                    builder.put(valueVectors[0]);
                    continue;
                }

                // .. else build the resulting array from arguments
                IObjectVectorBuilder arrayBuilder = context.getVectorBuilderFactory()
                        .getObjectVectorBuilder(ResolvedType.of(Type.Any), rowCount * size);

                for (int k = 0; k < rowCount; k++)
                {
                    for (int j = 0; j < size; j++)
                    {
                        arrayBuilder.put(valueVectors[j].valueAsObject(k));
                    }
                }

                builder.put(arrayBuilder.build());
            }
        }
        return builder.build();
    }

    /** Scalar eval */
    static ValueVector evalScalar(TupleVector input, List<IExpression> arguments, IExecutionContext context)
    {
        ResolvedType type = getScalarType(arguments);

        int rowCount = input.getRowCount();
        int size = arguments.size();

        ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }

        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(type, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            IValueVectorBuilder arrayBuilder = context.getVectorBuilderFactory()
                    .getValueVectorBuilder(type.getSubType(), size);
            for (int j = 0; j < size; j++)
            {
                arrayBuilder.put(vectors[j], i);
            }
            builder.put(arrayBuilder.build());
        }

        return builder.build();
    }
}
