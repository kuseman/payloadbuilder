package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Concat function. Concatenates all arguments into a string */
class ConcatFunction extends ScalarFunctionInfo
{
    ConcatFunction()
    {
        super("concat", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public IExpression fold(IExecutionContext context, List<IExpression> arguments)
    {
        if (arguments.stream()
                .allMatch(IExpression::isConstant))
        {
            final int expressionSize = arguments.size();
            final ValueVector[] vectors = new ValueVector[expressionSize];
            for (int i = 0; i < expressionSize; i++)
            {
                vectors[i] = arguments.get(i)
                        .eval(null);
            }
            UTF8String string = concat(vectors, 0);
            return context.getExpressionFactory()
                    .createStringExpression(string);
        }
        return null;
    }

    @Override
    public Arity arity()
    {
        return Arity.AT_LEAST_TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final int expressionSize = arguments.size();
        // Folded arguments, return the argument
        if (expressionSize == 1)
        {
            return arguments.get(0)
                    .eval(input, context);
        }

        final ValueVector[] vectors = new ValueVector[expressionSize];
        for (int i = 0; i < expressionSize; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public UTF8String getString(int row)
            {
                return concat(vectors, row);
            }
        };
    }

    private static UTF8String concat(ValueVector[] vectors, int row)
    {
        int size = vectors.length;

        UTF8String string = null;
        List<UTF8String> strings = null;
        for (int i = 0; i < size; i++)
        {
            // Null values are ignored
            if (vectors[i].isNull(row))
            {
                continue;
            }
            else if (string == null)
            {
                string = vectors[i].getString(row);
                continue;
            }

            if (strings == null)
            {
                strings = new ArrayList<>(size);
                strings.add(string);
            }

            strings.add(vectors[i].getString(row));
        }

        if (strings == null)
        {
            return string != null ? string
                    : UTF8String.EMPTY;
        }

        return UTF8String.concat(UTF8String.EMPTY, strings);
    }
}
