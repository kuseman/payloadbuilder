package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

/** Operator the join two other operators using nested loop */
public class NestedLoopJoin implements Operator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    
    /* TODO: need to put this into context, state here wont work when the query plan will be cached */
    /* Statistics */
    private int executionCount;
    
    public NestedLoopJoin(
            String logicalOperator,
            Operator outer,
            Operator inner,
            BiPredicate<EvaluationContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows)
    {
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.predicate = predicate;
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Row contextParent = context.getParentRow();
        final Iterator<Row> it = outer.open(context);
        executionCount++;
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
                return setNext();
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (ii == null && !it.hasNext())
                    {
                        context.setParentRow(contextParent);
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = it.next();
                        context.setParentRow(currentOuter);
                        hit = false;
                    }

                    if (ii == null)
                    {
                        ii = inner.open(context);
                    }

                    if (!ii.hasNext())
                    {
                        if ((populating && (emitEmptyOuterRows || hit))
                            ||
                            (!populating && (emitEmptyOuterRows && !hit)))
                        {
                            next = currentOuter;
                        }

                        ii = null;
                        
                        currentOuter = null;
                        continue;
                    }

                    currentOuter.setPredicateParent(contextParent);
                    Row currentInner = ii.next();
                    currentInner.setPredicateParent(currentOuter);
                    
                    if (predicate == null || predicate.test(context.getEvaluationContext(), currentInner))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, populating);
                        if (populating)
                        {
                            next = null;
                        }
                        hit = true;
                    }
                    
                    currentInner.clearPredicateParent();
                    currentOuter.clearPredicateParent();
                }

                return true;
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
        if (obj instanceof NestedLoopJoin)
        {
            NestedLoopJoin that = (NestedLoopJoin) obj;
            return outer.equals(that.outer)
                &&
                inner.equals(that.inner)
                &&
                Objects.equals(predicate, that.predicate)
                &&
                rowMerger.equals(that.rowMerger)
                &&
                populating == that.populating
                &&
                emitEmptyOuterRows == that.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("NESTED LOOP (%s) (POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, PREDICATE: %s)",
                logicalOperator,
                populating,
                emitEmptyOuterRows,
                executionCount,
                predicate);
        return description + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
