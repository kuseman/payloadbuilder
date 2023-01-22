package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** An aliased expression */
public class AliasExpression implements IExpression, HasAlias
{
    private final IExpression expression;
    private final String alias;
    private final String outputAlias;
    private final boolean internal;

    public AliasExpression(IExpression expression, String alias)
    {
        this(expression, alias, false);
    }

    public AliasExpression(IExpression expression, String alias, boolean internal)
    {
        this(expression, alias, "", internal);
    }

    public AliasExpression(IExpression expression, String alias, String outputAlias, boolean internal)
    {
        this.expression = fold(requireNonNull(expression, "expression"));
        this.alias = requireNonNull(alias, "alias");
        this.outputAlias = requireNonNull(outputAlias, "outputAlias");
        this.internal = internal;
    }

    public String getAliasString()
    {
        return alias;
    }

    public String getOutputAlias()
    {
        return outputAlias;
    }

    @Override
    public Alias getAlias()
    {
        return new Alias(alias, outputAlias);
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public ColumnReference getColumnReference()
    {
        return expression.getColumnReference();
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public ResolvedType getType()
    {
        return expression.getType();
    }

    @Override
    public boolean isInternal()
    {
        return internal;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return expression.eval(input, context);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        if (visitor instanceof ICoreExpressionVisitor)
        {
            return ((ICoreExpressionVisitor<T, C>) visitor).visit(this, context);
        }
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
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof AliasExpression)
        {
            AliasExpression that = (AliasExpression) obj;
            return expression.equals(that.expression)
                    && alias.equals(that.alias)
                    && outputAlias.equals(that.outputAlias)
                    && internal == that.internal;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toString() + " AS " + alias;
    }

    @Override
    public String toVerboseString()
    {
        return "[" + expression.toVerboseString()
               + "] AS "
               + alias
               + (!StringUtils.isBlank(outputAlias) ? " (" + outputAlias + ")"
                       : "");
    }

    private static IExpression fold(IExpression expression)
    {
        // A nested alias expression can be simplified by removing the middle alias
        // since that will never be used
        if (expression instanceof AliasExpression)
        {
            return ((AliasExpression) expression).getExpression();
        }
        return expression;
    }
}
