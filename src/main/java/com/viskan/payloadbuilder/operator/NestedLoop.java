package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;

/** Operator the join two other operators using nested loop */
public class NestedLoop implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final Predicate<Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    
    public NestedLoop(Operator outer, Operator inner, Predicate<Row> predicate, boolean populating)
    {
        this(outer, inner, predicate, DefaultRowMerger.DEFAULT, populating);
    }
    
    public NestedLoop(Operator outer, Operator inner, Predicate<Row> predicate, RowMerger rowMerger, boolean populating)
    {
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.predicate = requireNonNull(predicate, "predicate");
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
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
            boolean hit;

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
                    if (ii == null && !it.hasNext())
                    {
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = it.next();
                        hit = false;
                    }

                    if (ii == null)
                    {
                        ii = inner.open(context);
                    }

                    if (!ii.hasNext())
                    {
                        ii = null;
                        if (populating && hit)
                        {
                            next = currentOuter;
                        }
                            
                        currentOuter = null;
                        continue;
                    }

                    Row currentInner = ii.next();
                    if (currentInner.evaluatePredicate(currentOuter, predicate))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, populating);
                        if (populating)
                        {
                            next = null;
                        }
                        hit = true;
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
        return "NESTED LOOP" + (populating ? " (POPULATE)" : "") + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
