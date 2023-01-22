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

/** Substring function */
class SubstringFunction extends ScalarFunctionInfo
{
    SubstringFunction(Catalog catalog)
    {
        super(catalog, "substring", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns part of a string according to provided arguments." + System.lineSeparator() + "Ex. substring(<expression>, <start expression> [, <length expression>] ) ";
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        if (arguments.size() < 2)
        {
            throw new IllegalArgumentException("Function " + getName() + " expects at least 2 arguments.");
        }

        final ValueVector value = arguments.get(0)
                .eval(input, context);

        final ValueVector start = arguments.get(1)
                .eval(input, context);

        final ValueVector length = arguments.size() > 2 ? arguments.get(2)
                .eval(input, context)
                : null;

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
                return value.isNull(row)
                        || start.isNull(row)
                        || (length != null
                                && length.isNull(row));
            }

            @Override
            public UTF8String getString(int row)
            {
                String strArg = value.getString(row)
                        .toString();
                int startArg = start.getInt(row);

                if (length != null)
                {
                    int lengthArg = length.getInt(row);
                    return UTF8String.from(strArg.substring(startArg, startArg + lengthArg));
                }

                return UTF8String.from(strArg.substring(startArg));
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
}
