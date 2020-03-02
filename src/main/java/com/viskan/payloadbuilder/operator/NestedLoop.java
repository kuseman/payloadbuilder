package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

/** Operator the join two other operators using nested loop */
public class NestedLoop implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final BiPredicate<Row, Row> predicate;
    private final BiFunction<Row, Row, Row> rowMerger;

    public NestedLoop(Operator outer, Operator inner, BiPredicate<Row, Row> predicate, BiFunction<Row, Row, Row> rowMerger)
    {
        this.outer = requireNonNull(outer);
        this.inner = requireNonNull(inner);
        this.predicate = requireNonNull(predicate);
        this.rowMerger = requireNonNull(rowMerger);
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Iterator<Row> it = outer.open(context);
        return new Iterator<Row>()
        {
            Row next;
            Row currentOuter;
            Iterator<Row> ii;

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }

            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (!it.hasNext())
                    {
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = it.next();
                    }

                    if (ii == null)
                    {
                        ii = inner.open(context);
                    }

                    if (!ii.hasNext())
                    {
                        ii = null;
                        currentOuter = null;
                        continue;
                    }

                    Row currentInner = ii.next();

                    if (predicate.test(currentOuter, currentInner))
                    {
                        next = rowMerger.apply(currentOuter, currentInner);
                    }
                }

                return next != null;
            }
        };
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "NESTED LOOP" + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
