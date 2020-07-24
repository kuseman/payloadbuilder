package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Use statement */
public class UseStatement extends Statement
{
    private final QualifiedName qname;
    private final Expression expression;
    
    UseStatement(QualifiedName qname, Expression expression)
    {
        this.qname = requireNonNull(qname, "qname");
        this.expression = expression;
    }
    
    public QualifiedName getQname()
    {
        return qname;
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
