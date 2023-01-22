package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Returns type of provided argument */
class TypeOfFunction extends ScalarFunctionInfo
{
    TypeOfFunction(Catalog catalog)
    {
        super(catalog, "typeof", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns type string of provided argument. " + System.lineSeparator() + "Ex. typeof(expression)" + System.lineSeparator() + "Mainly used when debugging values.";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
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
            public boolean isNullable()
            {
                return false;
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
                    Object obj = (value.isNullable()
                            && value.isNull(row)) ? null
                                    : value.getValue(row);

                    String reflectType = obj == null ? "null"
                            : (obj.getClass()
                                    .getSimpleName());

                    return UTF8String.from("Any<" + reflectType + ">");
                }

                return UTF8String.from(value.type()
                        .toTypeString());
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
    //
    // @Override
    // public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    // {
    //
    // // Object obj = arguments.get(0)
    // // .eval(context);
    // // return obj == null ? null
    // // : obj.getClass()
    // // .getSimpleName();
    // }
}
