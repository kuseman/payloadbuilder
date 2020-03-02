package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

/** Base class for expressions */
public abstract class Expression
{
    public abstract <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context);
    
    public abstract boolean isNullable();
    
    public Class<?> getDataType()
    {
        return Object.class;
    }
    
    /** Generate code for this expression
     * @param context Context used during code generation
     * @param parentCode Code generated from parent expression 
     **/
    public abstract ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode);
    
    /**
     * Evaluate this expression with provided row  
     */
    public Object eval(Object evaluationContext, Row row)
    {
        return null;
    }
}
