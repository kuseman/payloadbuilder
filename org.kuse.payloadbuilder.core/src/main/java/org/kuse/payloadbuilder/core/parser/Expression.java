package org.kuse.payloadbuilder.core.parser;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

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

    /**
     * Generate code for this expression
     *
     * @param context Context used during code generation
     * @param parentCode Code generated from parent expression
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        throw new NotImplementedException("code gen: " + getClass().getSimpleName());
    }

    /**
     * Evaluate this expression with context
     *
     * @param context Context used during evaluation
     */
    public Object eval(ExecutionContext context)
    {
        throw new NotImplementedException("Not implemented. eval: " + getClass().getSimpleName());
    }
}
