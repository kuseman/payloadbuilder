package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns type of provided argument */
class TypeOfFunction extends ScalarFunctionInfo
{
    TypeOfFunction()
    {
        super("typeof", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns type string of provided argument. " + System.lineSeparator() + "Ex. typeof(expression)" + System.lineSeparator() + "Mainly used when debugging values.";
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
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
                if (value.type()
                        .getType() == Type.Any)
                {
                    // Reflectively resolve the underlaying value
                    Object obj = value.valueAsObject(row);

                    String reflectType = obj == null ? "null"
                            : (obj.getClass()
                                    .getSimpleName());

                    return UTF8String.from("Any<" + reflectType + ">");
                }

                return UTF8String.from(value.type()
                        .toTypeString());
            }
        };
    }
}
