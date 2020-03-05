package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

/** Definition of a scalar function */
public abstract class ScalarFunctionInfo extends FunctionInfo
{
//    private final Catalog catalog;
//    private final String name;

    public ScalarFunctionInfo(Catalog catalog, String name, Type type)
    {
        super(catalog, name, type);
//        this.catalog = requireNonNull(catalog, "catalog");
//        this.name = requireNonNull(name, "name");
    }
    
//    public String getName()
//    {
//        return name;
//    }
//    
//    /** Returns expression types wanted as input */
//    public List<Class<? extends Expression>> getInputTypes()
//    {
//        return null;
//    }
//    
//    /** Data type of this function */
//    public Class<?> getDataType()
//    {
//        return Object.class;
//    }
    
    /** Evaluate this function */
    @SuppressWarnings("unused")
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        throw new NotImplementedException("eval: " + getClass().getSimpleName());
    }
    
    /** Generate code for this function. Default is fallback to eval. 
     * @param context Context used during evaluation 
     * @param parentCode Expression code from parent node
     * @param arguments Arguments to function
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        throw new NotImplementedException("generate code: " + getClass().getSimpleName());
    }

//    /** Is this function nullable */
//    public boolean isNullable()
//    {
//        return true;
//    }
//    
//    @Override
//    public String toString()
//    {
//        return (DefaultCatalog.NAME.equals(catalog.getName()) ? "" : (catalog.getName() + ".")) + name;
//    }
}
