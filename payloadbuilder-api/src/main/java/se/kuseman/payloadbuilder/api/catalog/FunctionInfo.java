package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Base class for functions */
public abstract class FunctionInfo
{
    private final Catalog catalog;
    private final String name;
    private final FunctionType type;

    public FunctionInfo(Catalog catalog, String name, FunctionType type)
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

    public FunctionType getFunctionType()
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
                    && name.equals(fi.name)
                    && type.equals(fi.type);
        }
        return false;
    }

    /** Function type */
    public enum FunctionType
    {
        /** A scalar function used in expressions returning a scalar value. */
        SCALAR,
        /** A scalar function used in aggregations returning a scalar value from groups of values */
        AGGREGATE,
        /** A scalar function that act as both scalar and aggregate depending on context. */
        SCALAR_AGGREGATE,
        /** A table valued function used in function scans return a stream of {@link TupleVector} */
        TABLE,
        /** An operator function used in operators that supports transforming input stream to a scalar value. Ie. FOR clause */
        OPERATOR;

        /** Return true if this type is of aggregate type */
        public boolean isAggregate()
        {
            return this == AGGREGATE
                    || this == SCALAR_AGGREGATE;
        }
    }
}
