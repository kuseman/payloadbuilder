package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first non null argument */
class CoalesceFunction extends ScalarFunctionInfo
{
    CoalesceFunction()
    {
        super("coalesce", FunctionType.SCALAR);
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
    public ResolvedType getType(List<IExpression> arguments)
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
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ResolvedType type = getType(arguments);
        int size = arguments.size();
        ValueVector[] values = new ValueVector[size];
        int rowCount = input.getRowCount();
        IValueVectorBuilder builder = context.getVectorBuilderFactory()
                .getValueVectorBuilder(type, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            boolean allNull = true;
            for (int j = 0; j < size; j++)
            {
                if (values[j] == null)
                {
                    values[j] = arguments.get(j)
                            .eval(input, context);
                }
                if (!values[j].isNull(i))
                {
                    allNull = false;
                    builder.put(values[j], i);
                    break;
                }
            }
            if (allNull)
            {
                builder.putNull();
            }
        }
        return builder.build();
    }
}
