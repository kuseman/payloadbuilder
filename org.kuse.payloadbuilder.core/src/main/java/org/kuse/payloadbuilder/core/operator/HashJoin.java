package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.DescribeUtils.INNER_HASH_TIME;
import static org.kuse.payloadbuilder.core.DescribeUtils.INNER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.LOGICAL_OPERATOR;
import static org.kuse.payloadbuilder.core.DescribeUtils.OUTER_HASH_TIME;
import static org.kuse.payloadbuilder.core.DescribeUtils.OUTER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.POPULATING;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE_TIME;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.operator.StatementContext.NodeData;

/**
 * Hash match operator. Hashes outer operator and probes the inner operator
 */
class HashJoin extends AOperator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final ToIntBiFunction<ExecutionContext, Tuple> outerHashFunction;
    private final ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction;
    private final Predicate<ExecutionContext> predicate;
    private final TupleMerger tupleMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    HashJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ToIntBiFunction<ExecutionContext, Tuple> outerHashFunction,
            ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction,
            Predicate<ExecutionContext> predicate,
            TupleMerger tupleMerger,
            boolean populating,
            boolean emitEmptyOuterRows)
    {
        super(nodeId);
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerHashFunction = requireNonNull(outerHashFunction, "outerHashFunction");
        this.innerHashFunction = requireNonNull(innerHashFunction, "innerHashFunction");
        this.predicate = requireNonNull(predicate, "predicate");
        this.tupleMerger = requireNonNull(tupleMerger, "tupleMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    @Override
    public String getName()
    {
        return "Hash Join";
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return asList(outer, inner);
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        Data data = context.getStatementContext().getNodeData(nodeId);

        Map<String, Object> result = ofEntries(true,
                entry(LOGICAL_OPERATOR, logicalOperator),
                entry(POPULATING, populating),
                entry(PREDICATE, predicate),
                entry(OUTER_VALUES, outerHashFunction),
                entry(INNER_VALUES, innerHashFunction));

        if (data != null)
        {
            result.put(INNER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.innerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(OUTER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.outerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(PREDICATE_TIME, DurationFormatUtils.formatDurationHMS(data.predicateTime.getTime(TimeUnit.MILLISECONDS)));
        }

        return result;
    }

    @Override
    public TupleIterator open(ExecutionContext context)
    {
        Tuple contextOuter = context.getStatementContext().getTuple();
        JoinTuple joinTuple = new JoinTuple(contextOuter);
        Data data = context.getStatementContext().getOrCreateNodeData(nodeId, Data::new);
        Map<IntKey, List<TupleHolder>> table = hash(context, joinTuple, data);
        if (table.isEmpty())
        {
            return TupleIterator.EMPTY;
        }

        boolean markOuterRows = populating || emitEmptyOuterRows;
        TupleIterator probeIterator = probeIterator(joinTuple, table, context, data, markOuterRows);

        if (populating)
        {
            // Populate inner rows
            while (probeIterator.hasNext())
            {
                probeIterator.next();
            }
            probeIterator.close();
            return tableIterator(table, emitEmptyOuterRows ? TableIteratorType.BOTH : TableIteratorType.MATCHED);
        }

        if (!emitEmptyOuterRows)
        {
            return probeIterator;
        }

        // Left join
        // 1. Probe matched rows
        // 2. Probe non matched rows from table

        final TupleIterator it1 = probeIterator;
        final TupleIterator it2 = tableIterator(table, TableIteratorType.NON_MATCHED);
        //CSOFF
        return new TupleIterator()
        //CSON
        {
            TupleIterator current = it1;
            Tuple next;

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!current.hasNext())
                    {
                        if (current == it2)
                        {
                            return false;
                        }
                        current = it2;
                    }
                    else
                    {
                        next = current.next();
                    }
                }

                return true;
            }

            @Override
            public void close()
            {
                probeIterator.close();
            }
        };
    };

    private Map<IntKey, List<TupleHolder>> hash(ExecutionContext context, JoinTuple joinTuple, Data data)
    {
        IntKey key = new IntKey();
        Map<IntKey, List<TupleHolder>> table = new LinkedHashMap<>();
        TupleIterator oi = new ATupleIterator(outer.open(context));
        while (oi.hasNext())
        {
            Tuple tuple = oi.next();
            joinTuple.setInner(tuple);
            data.outerHashTime.resume();
            try
            {
                key.key = outerHashFunction.applyAsInt(context, joinTuple);
                List<TupleHolder> list = table.get(key);
                if (list == null)
                {
                    // Start with singleton list
                    list = singletonList(new TupleHolder(tuple));
                    table.put(key, list);
                    continue;
                }
                else if (list.size() == 1)
                {
                    // Convert to array list
                    list = new ArrayList<>(list);
                    table.put(key, list);
                }
                list.add(new TupleHolder(tuple));
            }
            finally
            {
                data.outerHashTime.suspend();
            }
        }
        oi.close();
        return table;
    }

    private TupleIterator probeIterator(
            JoinTuple joinTuple,
            Map<IntKey, List<TupleHolder>> table,
            ExecutionContext context,
            Data data,
            boolean markOuterRows)
    {
        final TupleIterator ii = new ATupleIterator(inner.open(context));
        //CSOFF
        return new TupleIterator()
        //CSON
        {
            private Tuple next;
            private Tuple currentInner;
            private List<TupleHolder> outerList;
            private int outerIndex;
            private final IntKey key = new IntKey();

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Tuple next()
            {
                Tuple tuple = next;
                next = null;
                return tuple;
            }

            @Override
            public void close()
            {
                ii.close();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (currentInner == null)
                    {
                        if (!ii.hasNext())
                        {
                            return false;
                        }

                        currentInner = ii.next();
                        joinTuple.setInner(currentInner);

                        data.innerHashTime.resume();
                        key.key = innerHashFunction.applyAsInt(context, joinTuple);
                        List<TupleHolder> list = table.get(key);
                        data.innerHashTime.suspend();
                        if (list == null)
                        {
                            currentInner = null;
                            continue;
                        }
                        outerList = list;
                        outerIndex = 0;
                        continue;
                    }

                    if (outerIndex >= outerList.size())
                    {
                        outerList = null;
                        currentInner = null;
                        continue;
                    }

                    TupleHolder currentOuter = outerList.get(outerIndex++);
                    joinTuple.setOuter(currentOuter.tuple);

                    data.predicateTime.resume();
                    context.getStatementContext().setTuple(joinTuple);
                    boolean result = predicate.test(context);
                    context.getStatementContext().setTuple(null);
                    data.predicateTime.suspend();

                    if (result)
                    {
                        next = tupleMerger.merge(currentOuter.tuple, currentInner, populating);
                        if (populating)
                        {
                            currentOuter.tuple = next;
                        }
                        if (markOuterRows)
                        {
                            currentOuter.match = true;
                        }
                    }
                }

                return true;
            }
        };
    }

    private TupleIterator tableIterator(
            Map<IntKey, List<TupleHolder>> table,
            TableIteratorType type)
    {
        final Iterator<List<TupleHolder>> tableIt = table.values().iterator();
        //CSOFF
        return new TupleIterator()
        //CSON
        {
            private Tuple next;
            private List<TupleHolder> list;
            private int index;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (list == null)
                    {
                        if (!tableIt.hasNext())
                        {
                            return false;
                        }

                        list = tableIt.next();
                        index = 0;
                        continue;
                    }
                    else if (index >= list.size())
                    {
                        list = null;
                        continue;
                    }

                    TupleHolder holder = list.get(index++);
                    if ((type == TableIteratorType.MATCHED && holder.match)
                        ||
                        type == TableIteratorType.NON_MATCHED && !holder.match
                        ||
                        type == TableIteratorType.BOTH)
                    {
                        next = holder.tuple;
                    }
                }
                return true;
            }
        };
    }

    /** Tuple holder */
    private static class TupleHolder
    {
        private Tuple tuple;
        private boolean match;

        TupleHolder(Tuple tuple)
        {
            this.tuple = tuple;
        }
    }

    /**
     * <pre>
     * NOTE! Special key class used in hash map.
     * To avoid allocations and use int as hashcode for rows, one instance of this class is used for all rows.
     * Since hashcode is all that is needed to find potential matching rows this technique is used
     * </pre>
     **/
    private static class IntKey
    {
        private int key;

        @Override
        public int hashCode()
        {
            return key;
        }

        @Override
        public boolean equals(Object obj)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return String.valueOf(key);
        }
    }

    /** Iterator type */
    private enum TableIteratorType
    {
        MATCHED,
        NON_MATCHED,
        BOTH;
    }

    /** Node data */
    private static class Data extends NodeData
    {
        StopWatch predicateTime = new StopWatch();
        StopWatch innerHashTime = new StopWatch();
        StopWatch outerHashTime = new StopWatch();

        Data()
        {
            predicateTime.start();
            predicateTime.suspend();

            innerHashTime.start();
            innerHashTime.suspend();

            outerHashTime.start();
            outerHashTime.suspend();
        }
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
        if (obj instanceof HashJoin)
        {
            HashJoin that = (HashJoin) obj;
            return nodeId == that.nodeId
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && outerHashFunction.equals(that.outerHashFunction)
                && innerHashFunction.equals(that.innerHashFunction)
                && predicate.equals(that.predicate)
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
        String desc = String.format("HASH JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, OUTER KEYS: %s, INNER KEYS: %s, PREDICATE: %s, MERGER: %s)",
                logicalOperator,
                nodeId,
                populating,
                emitEmptyOuterRows,
                null,
                outerHashFunction.toString(),
                innerHashFunction.toString(),
                predicate.toString(),
                tupleMerger.toString());
        return desc + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
