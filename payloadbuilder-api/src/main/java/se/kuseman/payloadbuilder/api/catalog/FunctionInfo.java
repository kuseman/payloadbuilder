package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.expression.IExpression;

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

    /** Return this functions arity. -1 if unknown */
    public int arity()
    {
        return -1;
    }

    /**
     * Fold arguments. Is called upon parsing to let functions fold it's arguments. Ie. Replace arguments with other values etc.
     * 
     * <pre>
     * NOTE! Arguments are unresolved so for example transforming a
     * QualifiedRefernece then argument will be UnresolvedQualifiedReferenceExpression
     * </pre>
     */
    public List<? extends IExpression> foldArguments(List<? extends IExpression> arguments)
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return catalog.getName() + "." + name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof FunctionInfo)
        {
            FunctionInfo fi = (FunctionInfo) obj;
            return catalog.getName()
                    .equals(fi.getCatalog()
                            .getName())
                    && name.equals(fi.getName())
                    && type.equals(fi.getType());
        }
        return false;
    }

    /** Function type */
    public enum Type
    {
        SCALAR,
        TABLE
    }
}
