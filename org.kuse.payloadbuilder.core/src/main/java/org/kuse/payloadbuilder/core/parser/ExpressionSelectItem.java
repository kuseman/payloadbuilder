package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/** Select item that is built from an expression */
public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;
    private final String assignmentName;

    public ExpressionSelectItem(Expression expression, String identifier, QualifiedName assignmentQname)
    {
        super(getIdentifier(expression, identifier), identifier != null);
        this.expression = requireNonNull(expression, "expression");
        this.assignmentName = assignmentQname != null ? join(assignmentQname.getParts(), ".") : null;
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
