package org.kuse.payloadbuilder.core.catalog.system;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Length of string function */
class LengthFunction extends ScalarFunctionInfo
{
    LengthFunction(Catalog catalog)
    {
        super(catalog, "length");
    }

    @Override
    public String getDescription()
    {
        return "Returns length of provided arguemtn in string form." + System.lineSeparator()
            + "Ex. length(<expression>) " + System.lineSeparator()
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
        return value.length();
    }
}
