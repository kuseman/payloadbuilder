package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import org.apache.commons.lang3.NotImplementedException;

/** Base class for expressions */
public abstract class Expression
{
    public abstract <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context);
    
    /** Returns true if the result from this expression is nullable. Primitive result or not */
    public abstract boolean isNullable();
    
    public Class<?> getDataType()
    {
        return Object.class;
    }
    
    /** Fold's expression */
    public Expression fold()
    {
        return this;
    }
    
    /** Returns true if this expression is constant or not */
    public boolean isConstant()
    {
        return false;
    }
    
    /** Generate code for this expression
     * @param context Context used during code generation
     * @param parentCode Code generated from parent expression 
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        throw new NotImplementedException("code gen: " + getClass().getSimpleName());
    }
    
    /**
     * Evaluate this expression with provided row  
     * @param evaluationContext Context used during evaluation
     * @param row Row used as evaluation source
     */
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        throw new NotImplementedException("eval: " + getClass().getSimpleName());
    }
}
