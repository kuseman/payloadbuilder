package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.ObjectUtils;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;

/** A resolved function (scalar, aggregate) call expression. Name and arguments. Used in physical plan when evaluating */
public class FunctionCallExpression implements IFunctionCallExpression, IAggregateExpression
{
    private final String catalogAlias;
    private final ScalarFunctionInfo function;
    private final AggregateMode aggregateMode;
    private final List<IExpression> arguments;

    public FunctionCallExpression(String catalogAlias, ScalarFunctionInfo function, AggregateMode aggregateMode, List<IExpression> arguments)
    {
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.function = requireNonNull(function, "function");
        this.aggregateMode = aggregateMode;
        this.arguments = ObjectUtils.defaultIfNull(arguments, emptyList());
    }

    @Override
    public ScalarFunctionInfo getFunctionInfo()
    {
        return function;
    }

    @Override
    public AggregateMode getAggregateMode()
    {
        return aggregateMode;
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    @Override
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
    public boolean isConstant()
    {
        // Consider all functions as non constant at the moment
        return false;
    }

    @Override
    public ResolvedType getType()
    {
        return function.getType(arguments);
    }

    @Override
    public ResolvedType getAggregateType()
    {
        return function.getAggregateType(arguments);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        if (aggregateMode != null)
        {
            return function.evalScalar(context, aggregateMode, input, catalogAlias, arguments);
        }

        return function.evalScalar(context, input, catalogAlias, arguments);
    }

    @Override
    public ValueVector eval(ValueVector groups, IExecutionContext context)
    {
        AggregateMode mode = this.aggregateMode != null ? this.aggregateMode
                : AggregateMode.ALL;
        return function.evalAggregate(context, mode, groups, catalogAlias, arguments);
    }

    @Override
    public int hashCode()
    {
        return function.hashCode();
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
        else if (obj instanceof FunctionCallExpression)
        {
            FunctionCallExpression that = (FunctionCallExpression) obj;
            return catalogAlias.equals(that.catalogAlias)
                    && function.equals(that.function)
                    && Objects.equals(aggregateMode, that.aggregateMode)
                    && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public boolean semanticEquals(IExpression expression)
    {
        if (equals(expression))
        {
            return true;
        }
        else if (expression instanceof FunctionCallExpression)
        {
            FunctionCallExpression that = (FunctionCallExpression) expression;
            // Same function and same catalog alias, then see if the arguments are the same
            if (catalogAlias.equals(that.catalogAlias)
                    && function.equals(that.function))
            {
                int size = arguments.size();
                if (size != that.arguments.size())
                {
                    return false;
                }

                for (int i = 0; i < size; i++)
                {
                    if (!arguments.get(i)
                            .semanticEquals(that.arguments.get(i)))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return function.getName() + "("
               + arguments.stream()
                       .map(IExpression::toString)
                       .collect(joining(", "))
               + ")";
    }

    @Override
    public String toVerboseString()
    {
        return function.getName() + "("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ")";
    }
}
