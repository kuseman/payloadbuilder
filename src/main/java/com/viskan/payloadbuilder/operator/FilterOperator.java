package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.BiPredicate;

import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang3.StringUtils;

/** Filtering operator */
class FilterOperator extends AOperator
{
    private final Operator operator;
    private final BiPredicate<ExecutionContext, Row> predicate;

    /* Statistics */
    private int executionCount;

    FilterOperator(int nodeId, Operator operator, BiPredicate<ExecutionContext, Row> predicate)
    {
        super(nodeId);
        this.operator = requireNonNull(operator);
        this.predicate = requireNonNull(predicate);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        executionCount++;
        return new FilterIterator(operator.open(context), i ->  predicate.test(context, (Row) i));
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
        if (obj instanceof FilterOperator)
        {
            FilterOperator that = (FilterOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator)
                && predicate.equals(that.predicate);
        }
        return false;
    }
    
    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("FILTER (ID: %d, EXECUTION COUNT: %s, PREDICATE: %s", nodeId, executionCount, predicate);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
