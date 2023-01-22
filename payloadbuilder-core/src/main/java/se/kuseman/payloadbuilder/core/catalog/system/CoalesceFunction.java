package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.function.IntFunction;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction(Catalog catalog)
    {
        super(catalog, "coalesce", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
               + "Ex. coalesce(expression1, expression2, expression3, ...)"
               + System.lineSeparator()
               + "If all arguments yield null, null is returned.";
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        // Find the highest precedence return type
        ResolvedType type = arguments.get(0)
                .getType();
        int size = arguments.size();
        for (int i = 1; i < size; i++)
        {
            ResolvedType t = arguments.get(i)
                    .getType();
            if (t.getType()
                    .getPrecedence() > type.getType()
                            .getPrecedence())
            {
                type = t;
            }
        }
        return type;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ResolvedType type = getType(arguments);
        final int size = arguments.size();

        return new ValueVectorAdapter(new IntFunction<ValueVector>()
        {
            ValueVector[] vectors = new ValueVector[size];
            ValueVector nullResult;

            @Override
            public ValueVector apply(int row)
            {
                for (int i = 0; i < size; i++)
                {
                    ValueVector current = vectors[i];
                    if (current == null)
                    {
                        current = arguments.get(i)
                                .eval(input, context);
                        vectors[i] = current;
                    }

                    if (!current.isNullable()
                            || !current.isNull(row))
                    {
                        return current;
                    }
                }

                if (nullResult == null)
                {
                    nullResult = ValueVector.literalNull(type, size);
                }
                return nullResult;
            }

        }, input.getRowCount(), true, type);
    }
}
