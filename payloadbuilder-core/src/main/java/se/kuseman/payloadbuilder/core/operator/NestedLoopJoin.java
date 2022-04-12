package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;

import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.NodeData;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.DescribeUtils;

/** Operator the join two other operators using nested loop */
class NestedLoopJoin extends AOperator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final Predicate<ExecutionContext> predicate;
    private final TupleMerger tupleMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    NestedLoopJoin(int nodeId, String logicalOperator, Operator outer, Operator inner, Predicate<ExecutionContext> predicate, TupleMerger tupleMerger, boolean populating, boolean emitEmptyOuterRows)
    {
        super(nodeId);
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.predicate = predicate;
        this.tupleMerger = requireNonNull(tupleMerger, "tupleMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    Operator getInner()
    {
        return inner;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return asList(outer, inner);
    }

    @Override
    public String getName()
    {
        return "Nested loop";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        Map<String, Object> result = ofEntries(entry(DescribableNode.PREDICATE, predicate == null ? ""
                : predicate.toString()), entry(DescribeUtils.POPULATING, populating));

        if (data != null)
        {
            result.put(DescribeUtils.PREDICATE_TIME, DurationFormatUtils.formatDurationHMS(data.predicateTime.getTime()));
        }
        return result;
    }

    @Override
    public TupleIterator open(IExecutionContext ctx)
    {
        final ExecutionContext context = (ExecutionContext) ctx;
        final Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, Data::new);
        final Tuple contextParent = context.getStatementContext()
                .getTuple();
        final JoinTuple joinTuple = new JoinTuple(contextParent);
        final TupleIterator it = new ATupleIterator(outer.open(context));
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            /** Single instance to avoid allocations */
            private Tuple next;
            private Tuple currentOuter;
            private TupleIterator ii;
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

            // CSOFF
            private boolean setNext()
            // CSON
            {
                while (next == null)
                {
                    if (ii == null
                            && !it.hasNext())
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
                        context.getStatementContext()
                                .setTuple(currentOuter);
                        ii = new ATupleIterator(inner.open(context));
                    }

                    if (!ii.hasNext())
                    {
                        if ((populating
                                && (emitEmptyOuterRows
                                        || hit))
                                || (!populating
                                        && (emitEmptyOuterRows
                                                && !hit)))
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

                    data.predicateTime.resume();
                    context.getStatementContext()
                            .setTuple(joinTuple);
                    boolean result = predicate == null
                            || predicate.test(context);
                    context.getStatementContext()
                            .setTuple(null);
                    data.predicateTime.suspend();

                    if (result)
                    {
                        next = tupleMerger.merge(currentOuter, currentInner, populating);
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

    /** Node data for nested loop */
    private static class Data extends NodeData
    {
        private final StopWatch predicateTime = new StopWatch();

        Data()
        {
            predicateTime.start();
            predicateTime.suspend();
        }
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + outer.hashCode();
        hashCode = hashCode * 37 + inner.hashCode();
        return hashCode;
        // CSON
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
                    && tupleMerger.equals(that.tupleMerger)
                    && populating == that.populating
                    && emitEmptyOuterRows == that.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("NESTED LOOP (%s) (ID: %d, POPULATING: %s, OUTER: %s, PREDICATE: %s)", logicalOperator, nodeId, populating, emitEmptyOuterRows, predicate);
        return description + System.lineSeparator() + indentString + outer.toString(indent + 1) + System.lineSeparator() + indentString + inner.toString(indent + 1);
    }
}
