package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.commons.collections.iterators.FilterIterator;

/** Filtering operator */
class Filter implements Operator
{
    private final Operator operator;
    private final Predicate<Row> predicate;

    Filter(Operator operator, Predicate<Row> predicate)
    {
        this.operator = requireNonNull(operator);
        this.predicate = requireNonNull(predicate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        return new FilterIterator(operator.open(context), i -> predicate.test((Row) i));
    }
}