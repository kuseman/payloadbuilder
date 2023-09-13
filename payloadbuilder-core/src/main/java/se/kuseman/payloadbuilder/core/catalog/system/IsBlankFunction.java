package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns first item that is not a blank string (null or empty) */
class IsBlankFunction extends ScalarFunctionInfo
{
    IsBlankFunction()
    {
        super("isblank", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns first non blank value of provided arguments. " + System.lineSeparator()
               + "Ex. isblank(expression1, expression2)"
               + System.lineSeparator()
               + "If both arguments is blank, second argument is returned. "
               + System.lineSeparator()
               + "NOTE! First argument is transfomed to a string to determine blank-ness.";
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);
        ValueVector arg1 = null;

        int rowCount = input.getRowCount();
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(getType(arguments), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            UTF8String str;
            if (value.isNull(i)
                    || UTF8String.EMPTY.equals(str = value.getString(i)))
            {
                if (arg1 == null)
                {
                    arg1 = arguments.get(1)
                            .eval(input, context);
                }
                if (arg1.isNull(i))
                {
                    builder.put(null);
                }
                else
                {
                    builder.put(arg1.getString(i));
                }
            }
            else
            {
                builder.put(str);
            }
        }
        return builder.build();
    }
}
