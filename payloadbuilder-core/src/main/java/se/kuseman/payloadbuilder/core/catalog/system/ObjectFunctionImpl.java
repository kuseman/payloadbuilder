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

        // Wrap the input as a single row
        return ValueVector.literalObject(ObjectVector.wrap(input), 1);
    }

    /** Aggregate eval */
    static ValueVector evalAggregate(ValueVector groups, List<IExpression> expressions, IExecutionContext context)
    {
        int groupSize = groups.size();

        ResolvedType type = getAggregateType(expressions);
        Schema schema = type.getSchema();

        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(type, groupSize);

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
                final List<ValueVector> vectors = new ArrayList<>(size / 2);

                for (int j = 0; j < size; j += 2)
                {
                    // Values are at the odd indices
                    vectors.add(expressions.get(j + 1)
                            .eval(group, context));
                }

                builder.put(ObjectVector.wrap(TupleVector.of(schema, vectors)));
            }
        }
        return builder.build();
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
