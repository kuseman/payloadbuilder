package com.viskan.payloadbuilder.parser;

import static org.apache.commons.lang3.StringUtils.join;

/** A variable (@var) */
public class VariableExpression extends Expression
{
    private final String name;

    public VariableExpression(QualifiedName qname)
    {
        this.name = join(qname.getParts(), ".");
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return true;
    }
    
    @Override
    public Object eval(ExecutionContext context)
    {
        return context.getVariable(name);
    }
    
    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
 
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof VariableExpression)
        {
            VariableExpression that = (VariableExpression) obj;
            return name.equals(that.name);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return ":" + name;
    }
}
