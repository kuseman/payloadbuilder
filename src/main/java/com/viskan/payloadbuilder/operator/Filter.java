package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.BiPredicate;

import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang3.StringUtils;

/** Filtering operator */
public class Filter implements Operator
{
    private final Operator operator;
    private final BiPredicate<EvaluationContext, Row> predicate;

    public Filter(Operator operator, BiPredicate<EvaluationContext, Row> predicate)
    {
        this.operator = requireNonNull(operator);
        this.predicate = requireNonNull(predicate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        return new FilterIterator(operator.open(context), i -> predicate.test(context.getEvaluationContext(), (Row) i));
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * operator.hashCode() +
            37 * predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Filter)
        {
            Filter f = (Filter) obj;
            return operator.equals(f.operator)
                &&
                predicate.equals(f.predicate);
        }
        return false;
    }
    
    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "FILTER (" + predicate + ")" + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
