package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Lower and upper function */
class TrimFunction extends ScalarFunctionInfo
{
    private final Type type;

    TrimFunction(Catalog catalog, Type type)
    {
        super(catalog, type.name);
        this.type = type;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + type.descriptiveName + " string value of provided argument." + System.lineSeparator() + "If argument is non String the argument is returned as is.";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);
        if (obj == null)
        {
            return null;
        }

        String value = String.valueOf(obj);
        // CSOFF
        switch (type)
        // CSON
        {
            case BOTH:
                return StringUtils.trim(value);
            case LEFT:
                return StringUtils.stripStart(value, null);
            case RIGHT:
                return StringUtils.stripEnd(value, null);
        }

        return null;
    }

    /** Type */
    enum Type
    {
        BOTH("trim", "trimmed"),
        LEFT("ltrim", "left trimmed"),
        RIGHT("rtrim", "right trimmed");

        String name;
        String descriptiveName;

        Type(String name, String descriptiveName)
        {
            this.name = name;
            this.descriptiveName = descriptiveName;
        }
    }
}
