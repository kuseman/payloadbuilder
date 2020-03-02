package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog._default.DefaultCatalog;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.directory.api.util.exception.NotImplementedException;

/** Definition of a function */
public abstract class FunctionInfo
{
    private final Catalog catalog;
    private final String name;

    protected FunctionInfo(Catalog catalog, String name)
    {
        this.catalog = requireNonNull(catalog, "catalog");
        this.name = requireNonNull(name, "name");
    }
    
    public String getName()
    {
        return name;
    }
    
    /** Data type of this function */
    public Class<?> getDataType()
    {
        return Object.class;
    }
    
    /** Evaluate this function */
    @SuppressWarnings("unused")
    public Object eval(Row row)
    {
        throw new NotImplementedException();
    }
    
    /** Generate code for this function. Default is fallback to eval. 
     * @param context Context used during evaluation 
     * @param parentCode Expression code from parent node
     * @param arguments Arguments to function
     **/
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        throw new NotImplementedException();
    }

    /** Is this function nullable */
    public boolean isNullable()
    {
        return true;
    }
    
    @Override
    public String toString()
    {
        return (DefaultCatalog.NAME.equals(catalog.getName()) ? "" : (catalog.getName() + ".")) + name;
    }
}
