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
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Left and -right pad function */
class PadFunction extends ScalarFunctionInfo
{
    private final boolean left;

    PadFunction(boolean left)
    {
        super(left ? "lpad"
                : "rpad", FunctionType.SCALAR);
        this.left = left;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (left ? "left"
                : "right")
               + " padded string of first argument."
               + System.lineSeparator()
               + "with length of second argument."
               + System.lineSeparator()
               + "A optional third argument can be supplied for pad string (defaults to single white space). "
               + System.lineSeparator()
               + "Ex. "
               + (left ? "l"
                       : "r")
               + "pad(expression, integerExpression [, expression])"
               + System.lineSeparator()
               + "NOTE! First argument is converted to a string.";
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return new Arity(2, 3);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, final TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

        final ValueVector length = arguments.get(1)
                .eval(input, context);

        final ValueVector padValue = arguments.size() > 2 ? arguments.get(2)
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
                        || length.isNull(row);
            }

            @Override
            public UTF8String getString(int row)
            {
                // Boxing here for now
                String strArg = String.valueOf(value.valueAsObject(row));
                int lengthArg = length.getInt(row);

                String padString = " ";
                if (padValue != null)
                {
                    padString = String.valueOf(padValue.valueAsObject(row));
                }

                return UTF8String.from(left ? StringUtils.leftPad(strArg, lengthArg, padString)
                        : StringUtils.rightPad(strArg, lengthArg, padString));
            }
        };
    }
}
