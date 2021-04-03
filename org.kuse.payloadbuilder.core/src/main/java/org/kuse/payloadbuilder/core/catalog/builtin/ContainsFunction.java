package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionMath;

/** Function listOf. Creates a list of provided arguments */
class ContainsFunction extends ScalarFunctionInfo
{
    ContainsFunction(Catalog catalog)
    {
        super(catalog, "contains");
    }

    @Override
    public String getDescription()
    {
        return "Checks if provided collection contains value argument" + System.lineSeparator()
            + "ie. contains(<collection expression>, <value expression>)";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        // Function has a declared getInputTypes method that guards against wrong argument count
        Object arg0 = arguments.get(0).eval(context);
        Object arg1 = arguments.get(1).eval(context);

        if (arg0 instanceof Collection)
        {
            return ((Collection<Object>) arg0).contains(arg1);
        }
        else if (arg0 instanceof Iterator)
        {
            Iterator<Object> it = (Iterator<Object>) arg0;
            while (it.hasNext())
            {
                Object arg = it.next();
                if (ExpressionMath.eq(arg, arg1))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
