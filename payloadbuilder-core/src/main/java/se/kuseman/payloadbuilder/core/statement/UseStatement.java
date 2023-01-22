package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Use statement */
public class UseStatement extends Statement
{
    private final QualifiedName qname;
    private final IExpression expression;

    public UseStatement(QualifiedName qname, IExpression expression)
    {
        this.qname = requireNonNull(qname, "qname");
        this.expression = expression;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
