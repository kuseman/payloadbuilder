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

/** Left/right string function */
class LeftRightFunction extends ScalarFunctionInfo
{
    private final boolean left;

    LeftRightFunction(boolean left)
    {
        super(left ? "left"
                : "right", FunctionType.SCALAR);
        this.left = left;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);
        ValueVector size = arguments.get(1)
                .eval(input, context);

        int rowCount = input.getRowCount();
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(ResolvedType.of(Type.String), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i)
                    || size.isNull(i))
            {
                builder.putNull();
            }
            else
            {
                int modSize = size.getInt(i);
                if (modSize < 0)
                {
                    throw new IllegalArgumentException("Function " + getName() + " expects a positive integer value for argument 2");
                }

                UTF8String string = value.getString(i);

                if (left)
                {
                    builder.put(UTF8String.from(StringUtils.left(string.toString(), modSize)));
                }
                else
                {
                    builder.put(UTF8String.from(StringUtils.right(string.toString(), modSize)));
                }
            }
        }
        return builder.build();
    }
}
