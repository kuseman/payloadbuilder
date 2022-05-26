package se.kuseman.payloadbuilder.core.parser;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** A back tick or template string expression. */
public class TemplateStringExpression extends Expression
{
    private final List<Expression> expressions;

    public TemplateStringExpression(List<Expression> expressions)
    {
        this.expressions = unmodifiableList(requireNonNull(expressions, "expressions"));
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        StringBuilder sb = new StringBuilder();
        for (Expression expression : expressions)
        {
            sb.append(expression.eval(context));
        }
        return sb.toString();
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Expression fold()
    {
        if (expressions.stream()
                .allMatch(Expression::isConstant))
        {
            return new LiteralStringExpression((String) eval(null));
        }

        return this;
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TemplateStringExpression)
        {
            TemplateStringExpression that = (TemplateStringExpression) obj;
            return expressions.equals(that.expressions);
        }
        return false;
    }
}
