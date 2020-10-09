package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Left and -right pad function */
class PadFunction extends ScalarFunctionInfo
{
    private final boolean left;

    PadFunction(Catalog catalog, boolean left)
    {
        super(catalog, left ? "leftpad" : "rightpad");
        this.left = left;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + (left ? "left" : "right") + " padded string of first argument." + System.lineSeparator()
        + "with length of second argument." + System.lineSeparator()
        + "A optional third argument can be supplied for pad string (defaults to single white space). " + System.lineSeparator()
        + "Ex. " + (left ? "left" : "right") + "pad(expression, integerExpression [, expression])" + System.lineSeparator()
        + "NOTE! First argument is converted to a string.";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object obj = arguments.get(0).eval(context);
        if (obj == null)
        {
            return null;
        }
        String value = String.valueOf(obj);

        Object lengthObj = arguments.get(1).eval(context);
        if (!(lengthObj instanceof Integer))
        {
            throw new IllegalArgumentException("Expected an integer expression for second argument of " + getName() + " but got " + lengthObj);
        }
        int length = ((Integer) lengthObj).intValue();

        if (arguments.size() >= 3)
        {
            Object padString = arguments.get(2).eval(context);
            return left
                ? StringUtils.leftPad(value, length, padString != null ? String.valueOf(padString) : " ")
                : StringUtils.rightPad(value, length, padString != null ? String.valueOf(padString) : " ");
        }

        return left
            ? StringUtils.leftPad(value, length)
            : StringUtils.rightPad(value, length);
    }
}
