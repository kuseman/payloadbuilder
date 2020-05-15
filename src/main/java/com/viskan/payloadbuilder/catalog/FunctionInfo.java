package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.catalog.builtin.BuiltinCatalog;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

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

    public Catalog getCatalog()
    {
        return catalog;
    }

    public Type getType()
    {
        return type;
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

    /** Is this function nullable */
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
                &&
                name.equals(fi.getName())
                &&
                type.equals(fi.getType());
        }
        return super.equals(obj);
    }

    public enum Type
    {
        SCALAR,
        TABLE
    }
}
