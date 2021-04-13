package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;

/** Select item that is built from an expression */
public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;
    private final String assignmentName;

    public ExpressionSelectItem(Expression expression, String identifier, QualifiedName assignmentQname, Token token)
    {
        super(getIdentifier(expression, identifier), isBlank(identifier), token);
        this.expression = requireNonNull(expression, "expression");
        this.assignmentName = assignmentQname != null ? join(assignmentQname.getParts(), ".") : null;
    }

    private static String getIdentifier(Expression expression, String identifier)
    {
        if (!isBlank(identifier))
        {
            return identifier;
        }

        if (expression instanceof HasIdentifier)
        {
            return ((HasIdentifier) expression).identifier();
        }

        return "";
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

    @SuppressWarnings("deprecation")
    @Override
    public List<ResolvePath> getResolvePaths()
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
        return !(expression instanceof QualifiedReferenceExpression);
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
