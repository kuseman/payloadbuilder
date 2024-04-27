package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first item if not null else second item */
class IsNullFunction extends ScalarFunctionInfo
{
    IsNullFunction()
    {
        super("isnull", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns first non null value of provided arguments. " + System.lineSeparator()
               + "Ex. isnull(expression, expression)"
               + System.lineSeparator()
               + "If both arguments yield null, null is returned.";
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        ResolvedType typeA = arguments.get(0)
                .getType();
        ResolvedType typeB = arguments.get(1)
                .getType();

        return typeA.getType()
                .getPrecedence() >= typeB.getType()
                        .getPrecedence() ? typeA
                                : typeB;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);
        ValueVector arg1 = null;

        ResolvedType type = getType(arguments);
        int rowCount = input.getRowCount();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(type, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i))
            {
                if (arg1 == null)
                {
                    arg1 = arguments.get(1)
                            .eval(input, context);
                }
                resultVector.copy(i, arg1, i);
            }
            else
            {
                resultVector.copy(i, value, i);
            }
        }
        return resultVector;
    }
}
