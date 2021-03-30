package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

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
    private final BiPredicate<ExecutionContext, Tuple> predicate;
    private final TupleMerger rowMerger;
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
            BiPredicate<ExecutionContext, Tuple> predicate,
            TupleMerger rowMerger,
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

    Operator getInner()
    {
        return inner;
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
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(
                entry(DescribeUtils.PREDICATE, predicate == null ? "" : predicate.toString()),
                entry(DescribeUtils.POPULATING, populating));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        /*
         * from tableA a
         * inner join tableB b
         *   on ...
         */

        final Tuple contextParent = context.getTuple();
        final JoinTuple joinTuple = new JoinTuple(contextParent);

        final RowIterator it = outer.open(context);
        executionCount++;
        //CSOFF
        return new RowIterator()
        //CSON
        {
            /** Single instance to avoid allocations */
            private Tuple next;
            private Tuple currentOuter;
            private RowIterator ii;
            private boolean hit;

            @Override
            public Tuple next()
            {
                Tuple t = next;
                next = null;
                return t;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public void close()
            {
                it.close();
            }

            //CSOFF
            private boolean setNext()
            //CSON
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
                        joinTuple.setOuter(currentOuter);
                        hit = false;
                    }

                    if (ii == null)
                    {
                        context.setTuple(currentOuter);
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

                        ii.close();
                        ii = null;

                        currentOuter = null;
                        continue;
                    }

                    Tuple currentInner = ii.next();
                    joinTuple.setInner(currentInner);
                    if (predicate == null || predicate.test(context, joinTuple))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, populating, nodeId);
                        if (populating)
                        {
                            currentOuter = next;
                            next = null;
                        }
                        hit = true;
                    }
                }

                return true;
            }
        };
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + outer.hashCode();
        hashCode = hashCode * 37 + inner.hashCode();
        return hashCode;
        //CSON
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
