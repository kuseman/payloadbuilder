package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.catalog.builtin.BuiltinCatalog;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.NamedExpression;
import org.kuse.payloadbuilder.core.parser.ParseException;

/** Definition of a function */
public abstract class FunctionInfo
{
    private final Catalog catalog;
    private final String name;
    private final Type type;

    public FunctionInfo(Catalog catalog, String name, Type type)
    {
        this.catalog = requireNonNull(catalog, "catalog");
        this.name = requireNonNull(name, "name");
        this.type = requireNonNull(type, "type");
    }

    public String getName()
    {
        return name;
    }

    /** Description of function. Used in show statement for a description of the function. */
    public String getDescription()
    {
        return "";
    }

    public Catalog getCatalog()
    {
        return catalog;
    }

    public Type getType()
    {
        return type;
    }

    /** Returns true if all arguments should be named for this function else false. */
    public boolean requiresNamedArguments()
    {
        return false;
    }

    /** Returns expression types wanted as input */
    public List<Class<? extends Expression>> getInputTypes()
    {
        return null;
    }

    /** Data type of this function */
    public Class<?> getDataType()
    {
        return Object.class;
    }

    /** Is the return value of this function nullable */
    public boolean isNullable()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return (BuiltinCatalog.NAME.equals(catalog.getName()) ? "" : (catalog.getName() + ".")) + name;
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * catalog.getName().hashCode()) +
            (37 * name.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof FunctionInfo)
        {
            FunctionInfo fi = (FunctionInfo) obj;
            return catalog.getName().equals(fi.getCatalog().getName())
                && name.equals(fi.getName())
                && type.equals(fi.getType());
        }
        return false;
    }

    /** Validate function info against arguments */
    public static void validate(FunctionInfo functionInfo, List<Expression> arguments, Token token)
    {
        if (functionInfo.getInputTypes() != null)
        {
            List<Class<? extends Expression>> inputTypes = functionInfo.getInputTypes();
            int size = inputTypes.size();
            if (arguments.size() != size)
            {
                throw new ParseException("Function " + functionInfo.getName() + " expected " + inputTypes.size() + " parameters, found " + arguments.size(), token);
            }
            for (int i = 0; i < size; i++)
            {
                Class<? extends Expression> inputType = inputTypes.get(i);
                if (!inputType.isAssignableFrom(arguments.get(i).getClass()))
                {
                    throw new ParseException(
                            "Function " + functionInfo.getName() + " expects " + inputType.getSimpleName() + " as parameter at index " + i + " but got "
                                + arguments.get(i).getClass().getSimpleName(),
                            token);
                }
            }
        }
        if (functionInfo.requiresNamedArguments() && (arguments.size() <= 0 || arguments.stream().anyMatch(a -> !(a instanceof NamedExpression))))
        {
            if (arguments.stream().anyMatch(a -> !(a instanceof NamedExpression)))
            {
                throw new ParseException(
                        "Function " + functionInfo.getName() + " expects named parameters", token);
            }
        }
    }

    public enum Type
    {
        SCALAR,
        TABLE
    }
}
