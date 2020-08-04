package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Lower and upper function */
class TrimFunction extends ScalarFunctionInfo
{
    private final Type type;
    TrimFunction(Catalog catalog, Type type)
    {
        super(catalog, type.name, FunctionInfo.Type.SCALAR);
        this.type = type;
    }

    @Override
    public String getDescription()
    {
        return "Returns " + type.descriptiveName + " string value of provided argument." + System.lineSeparator() +
                "If argument is non String the argument is returned as is.";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
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
        switch (type)
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
