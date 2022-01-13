package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Select item that is built from an expression */
public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;
    private final String assignmentName;

    public ExpressionSelectItem(Expression expression, boolean emptyIdentifier, String identifier, String assignmentQname, Token token)
    {
        super(identifier, emptyIdentifier, token);
        this.expression = requireNonNull(expression, "expression");
        this.assignmentName = assignmentQname;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public String getAssignmentName()
    {
        return assignmentName;
    }

    @Override
    public Object getAssignmentValue(ExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public ResolvePath[] getResolvePaths()
    {
        if (expression instanceof QualifiedReferenceExpression)
        {
            return ((QualifiedReferenceExpression) expression).getResolvePaths();
        }
        return null;
    }

    @Override
    public boolean isComputed()
    {
        return !(expression instanceof QualifiedReferenceExpression
            || expression instanceof UnresolvedQualifiedReferenceExpression
            || expression instanceof LiteralExpression);
    }

    @Override
    public QualifiedName getQname()
    {
        if (expression instanceof UnresolvedQualifiedReferenceExpression)
        {
            return ((UnresolvedQualifiedReferenceExpression) expression).getQname();
        }
        else if (expression instanceof QualifiedReferenceExpression)
        {
            ((QualifiedReferenceExpression) expression).getQname();
        }
        return null;
    }

    @Override
    public boolean isSubQuery()
    {
        return expression instanceof UnresolvedSubQueryExpression;
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
