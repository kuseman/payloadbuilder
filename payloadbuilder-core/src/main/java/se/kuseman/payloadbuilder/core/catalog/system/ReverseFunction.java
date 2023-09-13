package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Reverse string function */
class ReverseFunction extends ScalarFunctionInfo
{
    ReverseFunction()
    {
        super("reverse", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        int rowCount = input.getRowCount();
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(ResolvedType.of(Type.String), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i))
            {
                builder.putNull();
            }
            else
            {
                UTF8String string = value.getString(i);
                builder.put(UTF8String.from(StringUtils.reverse(string.toString())));
            }
        }
        return builder.build();
    }
}
