package org.kuse.payloadbuilder.core.parser;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Base class for expressions */
//CSOFF
public abstract class Expression
//CSON
{
    /** Visitor accept definition */
    public abstract <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context);

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

    /** Returns true if this expression supports code generation */
    public boolean isCodeGenSupported()
    {
        return false;
    }

    /**
     * Generate code for this expression
     *
     * @param context Context used during code generation
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context)
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
