package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator the join two other operators using nested loop */
class NestedLoopJoin extends AOperator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final BiPredicate<ExecutionContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    /* TODO: need to put this into context, state here wont work when the query plan will be cached */
    /* Statistics */
    private int executionCount;

    NestedLoopJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            BiPredicate<ExecutionContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows)
    {
        super(nodeId);
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.predicate = predicate;
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(outer, inner);
    }
    
    @Override
    public String getName()
    {
        return "Nested loop";
    }
    
    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(
                entry(DescribeUtils.PREDICATE, predicate),
                entry(DescribeUtils.POPULATING, populating));
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        final Row contextParent = context.getRow();
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
                        context.setRow(contextParent);
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = it.next();
                        context.setRow(currentOuter);
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

                    if (predicate == null || predicate.test(context, currentInner))
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
            return nodeId == that.nodeId
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && Objects.equals(predicate, that.predicate)
                && rowMerger.equals(that.rowMerger)
                && populating == that.populating
                && emitEmptyOuterRows == that.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("NESTED LOOP (%s) (ID: %d, POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, PREDICATE: %s)",
                logicalOperator,
                nodeId,
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
