package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;

/**
 * Implementation of function object. Creates an object from provided arguments.
 */
class ObjectFunctionImpl
{
    static ResolvedType getScalarType(List<IExpression> arguments)
    {
        int size = arguments.size();
        if (size > 0
                && size % 2 != 0)
        {
            throw new IllegalArgumentException("Function object requires an even number of arguments");
        }

        Schema schema = Schema.EMPTY;
        if (size > 0)
        {
            List<Column> columns = new ArrayList<>(size / 2);
            for (int i = 0; i < size; i += 2)
            {
                IExpression key = arguments.get(i);
                IExpression value = arguments.get(i + 1);
                if (!(key instanceof ILiteralStringExpression))
                {
                    throw new IllegalArgumentException("Function object requires literal string keys");
                }
                columns.add(Column.of(((ILiteralStringExpression) key).getValue()
                        .toString(), value.getType()));
            }
            schema = new Schema(columns);
        }
        return ResolvedType.object(schema);
    }

    static ResolvedType getOperatorType(Schema input)
    {
        return ResolvedType.object(input);
    }

    static ResolvedType getAggregateType(List<IExpression> arguments)
    {
        return getScalarType(arguments);
    }

    /** Operator eval */
    static ValueVector evalOperator(TupleVector input, IExecutionContext context)
    {
        int rowCount = input.getRowCount();
        if (rowCount == 0)
        {
            return ValueVector.literalNull(getOperatorType(input.getSchema()), 1);
        }

        TupleVectorBuilder builder = new TupleVectorBuilder(((ExecutionContext) context).getBufferAllocator(), 1);
        builder.append(input, 0);

        // Wrap the input as a single row
        return ValueVector.literalObject(ObjectVector.wrap(builder.build()), 1);
    }

    /** Scalar eval */
    static ValueVector evalScalar(TupleVector input, List<IExpression> arguments, IExecutionContext context)
    {
        ResolvedType type = getScalarType(arguments);

        int size = arguments.size();

        final List<ValueVector> vectors = new ArrayList<>(size / 2);

        // Values are at the odd indices
        for (int i = 0; i < size; i += 2)
        {
            vectors.add(arguments.get(i + 1)
                    .eval(input, context));
        }

        final Schema schema = type.getSchema();
        int rowCount = input.getRowCount();
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(type, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            final int row = i;
            builder.put(new ObjectVector()
            {
                @Override
                public int getRow()
                {
                    return row;
                }

                @Override
                public ValueVector getValue(int ordinal)
                {
                    return vectors.get(ordinal);
                }

                @Override
                public Schema getSchema()
                {
                    return schema;
                }
            });
        }

        return builder.build();
    }
}
