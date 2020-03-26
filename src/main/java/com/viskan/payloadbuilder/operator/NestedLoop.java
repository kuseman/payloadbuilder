package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

/** Operator the join two other operators using nested loop */
public class NestedLoop implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;

    public NestedLoop(Operator outer, Operator inner, BiPredicate<EvaluationContext, Row> predicate, boolean populating)
    {
        this(outer, inner, predicate, DefaultRowMerger.DEFAULT, populating);
    }

    public NestedLoop(Operator outer, Operator inner, BiPredicate<EvaluationContext, Row> predicate, RowMerger rowMerger, boolean populating)
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
                    if (currentInner.evaluatePredicate(currentOuter, context.getEvaluationContext(), predicate))
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
    public int hashCode()
    {
        return 17 + (outer.hashCode() * 37) + (inner.hashCode() * 37);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof NestedLoop)
        {
            NestedLoop that = (NestedLoop) obj;
            return outer.equals(that.outer)
                &&
                inner.equals(that.inner)
                &&
                predicate.equals(that.predicate)
                &&
                rowMerger.equals(that.rowMerger)
                &&
                populating == that.populating;
        }
        return false;
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
