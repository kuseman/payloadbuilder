package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Select item that is built from an expression */
public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;

    public ExpressionSelectItem(Expression expression, String identifier)
    {
        super(getIdentifier(expression, identifier), identifier != null);
        this.expression = requireNonNull(expression, "expression");
    }

    private static String getIdentifier(Expression expression, String identifier)
    {
        if (identifier != null)
        {
            return identifier;
        }

        if (expression instanceof QualifiedReferenceExpression)
        {
            return ((QualifiedReferenceExpression) expression).getQname().getLast();
        }
        else if (expression instanceof DereferenceExpression)
        {
            return ((DereferenceExpression) expression).getRight().getQname().getLast();
        }

        return "No column name";
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return expression.toString() + super.toString();
    }
}
