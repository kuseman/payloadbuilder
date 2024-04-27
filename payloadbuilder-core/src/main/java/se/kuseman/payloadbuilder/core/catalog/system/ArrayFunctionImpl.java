package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
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

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(type.getSubType(), rowCount * size);
        int index = 0;
        for (int k = 0; k < rowCount; k++)
        {
            for (int j = 0; j < size; j++)
            {
                if (vectors[j] == null)
                {
                    continue;
                }

                resultVector.copy(index++, vectors[j], k);
            }
        }

        return ValueVector.literalArray(resultVector, 1);
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

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(type, rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            MutableValueVector arrayVector = context.getVectorFactory()
                    .getMutableVector(type.getSubType(), size);

            for (int j = 0; j < size; j++)
            {
                arrayVector.copy(j, vectors[j], i);
            }
            resultVector.setArray(i, arrayVector);
        }

        return resultVector;
    }
}
