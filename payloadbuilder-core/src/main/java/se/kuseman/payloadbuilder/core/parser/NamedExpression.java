package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** A named expression used as function arguments etc. */
public class NamedExpression extends Expression implements INamedExpression
{
    private final String name;
    private final Expression expression;

    public NamedExpression(String name, Expression expression)
    {
        this.name = StringUtils.lowerCase(name);
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant();
    }

    @Override
    public DataType getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof NamedExpression)
        {
            NamedExpression that = (NamedExpression) obj;
            return Objects.equals(name, that.name)
                    && expression.equals(that.expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (name != null ? name + ": "
                : "") + expression;
    }
}
