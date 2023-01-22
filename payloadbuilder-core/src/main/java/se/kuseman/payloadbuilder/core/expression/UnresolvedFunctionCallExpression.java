package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.defaultString;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** An unresolved function (scalar, aggregate) call expression. Name and arguments */
public class UnresolvedFunctionCallExpression implements IExpression, IAggregateExpression
{
    private final String catalogAlias;
    private final String name;
    private final AggregateMode aggregateMode;
    private final List<IExpression> arguments;
    private final Token token;

    public UnresolvedFunctionCallExpression(String catalogAlias, String name, AggregateMode aggregateMode, List<IExpression> arguments, Token token)
    {
        this.catalogAlias = defaultString(catalogAlias, "");
        this.name = requireNonNull(name, "name");
        this.aggregateMode = aggregateMode;
        this.arguments = requireNonNull(arguments, "arguments");
        this.token = token;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getName()
    {
        return name;
    }

    public AggregateMode getAggregateMode()
    {
        return aggregateMode;
    }

    public Token getToken()
    {
        return token;
    }

    public List<IExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return arguments;
    }

    @Override
    public ResolvedType getType()
    {
        throw new IllegalArgumentException("An unresolved function call expression have no type");
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
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        throw new IllegalArgumentException("An unresolved function call expression should not be evaluated");
    }

    @Override
    public ValueVector eval(ValueVector groups, IExecutionContext context)
    {
        throw new IllegalArgumentException("An unresolved function call expression should not be evaluated");
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
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
        else if (obj instanceof UnresolvedFunctionCallExpression)
        {
            UnresolvedFunctionCallExpression that = (UnresolvedFunctionCallExpression) obj;
            return catalogAlias.equals(that.catalogAlias)
                    && name.equals(that.name)
                    && Objects.equals(aggregateMode, that.aggregateMode)
                    && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return ("".equals(catalogAlias) ? ""
                : catalogAlias + "#")
               + name
               + "("
               + arguments.stream()
                       .map(IExpression::toString)
                       .collect(joining(", "))
               + ")";
    }

    @Override
    public String toVerboseString()
    {
        return ("".equals(catalogAlias) ? ""
                : catalogAlias + "#")
               + name
               + "("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ")";
    }
}
