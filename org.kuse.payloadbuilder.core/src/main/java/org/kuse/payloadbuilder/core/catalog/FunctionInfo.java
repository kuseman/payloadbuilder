package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.catalog.builtin.BuiltinCatalog;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Definition of a function */
//CSOFF
public abstract class FunctionInfo
//CSON
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

    /** Data type of this function
     * @param arguments Function arguments
     */
    public DataType getDataType(List<Expression> arguments)
    {
        return DataType.ANY;
    }

    /**
     * Fold arguments. Is called upon parsing to let functions fold it's arguments. Ie. Replace arguments with other values etc.
     */
    public List<Expression> foldArguments(List<Expression> arguments)
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return (BuiltinCatalog.NAME.equals(catalog.getName()) ? "" : (catalog.getName() + ".")) + name;
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
            return catalog.getName().equals(fi.getCatalog().getName())
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
