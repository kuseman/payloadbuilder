package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Expression based predicate */
public class ExpressionPredicate implements BiFunction<TupleVector, IExecutionContext, ValueVector>
{
    private final IExpression predicate;

    public ExpressionPredicate(IExpression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate");
    }

    @Override
    public ValueVector apply(TupleVector t, IExecutionContext u)
    {
        return predicate.eval(t, u);
    }

    @Override
    public int hashCode()
    {
        return predicate.hashCode();
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
        else if (obj instanceof ExpressionPredicate)
        {
            ExpressionPredicate that = (ExpressionPredicate) obj;
            return predicate.equals(that.predicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return predicate.toString();
    }
}
