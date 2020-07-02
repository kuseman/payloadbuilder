package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/** Set statement */
public class SetStatement extends Statement
{
    private final String key;
    private final Expression expression;
    
    SetStatement(QualifiedName qname, Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
        String key;
        if (qname.getCatalog() != null)
        {
            key = qname.getCatalog() + "#" + join(qname.getParts(), ".");
        }
        else
        {
            key = join(qname.getParts(), ".");
        }
        this.key = key;
    }
    
    public String getKey()
    {
        return key;
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
