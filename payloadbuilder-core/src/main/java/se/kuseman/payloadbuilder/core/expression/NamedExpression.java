package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;

/** A named expression used as function arguments etc. */
public class NamedExpression implements INamedExpression
{
    private final String name;
    private final IExpression expression;

    public NamedExpression(String name, IExpression expression)
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
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public ResolvedType getType()
    {
        return expression.getType();
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return expression.eval(input, context);
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof NamedExpression)
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
