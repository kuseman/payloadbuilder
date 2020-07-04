package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/** Set statement */
public class SetStatement extends Statement
{
    private final String scope;
    private final String name;
    private final Expression expression;
    
    SetStatement(String scope, QualifiedName qname, Expression expression)
    {
        this.scope = scope;
        this.name = join(qname.getParts(), ".");
        this.expression = requireNonNull(expression, "expression");
    }
    
    public String getScope()
    {
        return scope;
    }
    
    public String getName()
    {
        return name;
    }
    
    public Expression getExpression()
    {
        return expression;
    }
    
    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
