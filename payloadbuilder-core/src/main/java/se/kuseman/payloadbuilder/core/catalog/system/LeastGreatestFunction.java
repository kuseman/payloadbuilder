package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** GREATES/LEAST function implementation. Returns the least/gratest from it's arguments */
class LeastGreatestFunction extends ScalarFunctionInfo
{
    private final boolean greatest;

    LeastGreatestFunction(boolean greatest)
    {
        super(greatest ? "greatest"
                : "least", FunctionType.SCALAR);
        this.greatest = greatest;
    }

    @Override
    public Arity arity()
    {
        return Arity.AT_LEAST_TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        ResolvedType result = arguments.get(0)
                .getType();
        int size = arguments.size();
        for (int i = 1; i < size; i++)
        {
            if (arguments.get(i)
                    .getType()
                    .getType()
                    .getPrecedence() > result.getType()
                            .getPrecedence())
            {
                result = arguments.get(i)
                        .getType();
            }
        }

        return result;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ResolvedType type = getType(arguments);
        int size = arguments.size();
        int rowCount = input.getRowCount();
        ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }

        MutableValueVector vectorResult = context.getVectorFactory()
                .getMutableVector(type, rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            int currentIndex = 0;
            for (int j = 1; j < size; j++)
            {
                // Current is null then move currentIndex to next
                if (vectors[currentIndex].isNull(i))
                {
                    currentIndex = j;
                    continue;
                }
                // Next is null => move on to next argument
                else if (vectors[j].isNull(i))
                {
                    continue;
                }

                int c = VectorUtils.compare(vectors[currentIndex], vectors[j], type.getType(), i, i);

                // Switch the current
                if ((greatest
                        && c < 0)
                        || (!greatest
                                && c > 0))
                {
                    currentIndex = j;
                }
            }

            vectorResult.copy(i, vectors[currentIndex], i);
        }

        return vectorResult;
    }
}
