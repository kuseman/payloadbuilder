package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.DescribeUtils.INNER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.LOGICAL_OPERATOR;
import static org.kuse.payloadbuilder.core.DescribeUtils.OUTER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.POPULATING;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

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
    private final BiPredicate<ExecutionContext, Tuple> predicate;
    private final TupleMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    HashJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ToIntBiFunction<ExecutionContext, Tuple> outerHashFunction,
            ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction,
            BiPredicate<ExecutionContext, Tuple> predicate,
            TupleMerger rowMerger,
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
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    @Override
    public String getName()
    {
        return "Hash Join";
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(outer, inner);
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(true,
                entry(LOGICAL_OPERATOR, logicalOperator),
                entry(POPULATING, populating),
                entry(PREDICATE, predicate),
                entry(OUTER_VALUES, outerHashFunction),
                entry(INNER_VALUES, innerHashFunction));
    }

    /** Node data */
    class Data extends NodeData
    {
        AtomicLong time = new AtomicLong();
        long innerHashTime;
        long outerHashTime;

        @Override
        public String toString()
        {
            return "Timings. innerTime: " + innerHashTime + ", outerTime: " + outerHashTime + "): " + DurationFormatUtils.formatDurationHMS(time.get());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowIterator open(ExecutionContext context)
    {
        Tuple contextOuter = context.getTuple();
        final JoinTuple joinTuple = new JoinTuple(contextOuter);
        Map<IntKey, List<TupleHolder>> table = hash(context, joinTuple);
        if (table.isEmpty())
        {
            return RowIterator.EMPTY;
        }

        boolean markOuterRows = populating || emitEmptyOuterRows;
        RowIterator probeIterator = probeIterator(joinTuple, table, context, markOuterRows);

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
        return RowIterator.wrap(new IteratorChain(
                probeIterator,
                tableIterator(table, TableIteratorType.NON_MATCHED)), () ->
                {
                    probeIterator.close();
                });
    };

    private Map<IntKey, List<TupleHolder>> hash(ExecutionContext context, JoinTuple joinTuple)
    {
        IntKey key = new IntKey();
        Map<IntKey, List<TupleHolder>> table = new LinkedHashMap<>();
        RowIterator oi = outer.open(context);
        while (oi.hasNext())
        {
            Tuple tuple = oi.next();
            joinTuple.setInner(tuple);
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
        oi.close();
        return table;
    }

    private RowIterator probeIterator(
            JoinTuple joinTuple,
            Map<IntKey, List<TupleHolder>> table,
            ExecutionContext context,
            boolean markOuterRows)
    {
        final RowIterator ii = inner.open(context);
        //CSOFF
        return new RowIterator()
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

                        key.key = innerHashFunction.applyAsInt(context, joinTuple);
                        List<TupleHolder> list = table.get(key);
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
                    if (predicate.test(context, joinTuple))
                    {
                        next = rowMerger.merge(currentOuter.tuple, currentInner, populating, nodeId);
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

    private RowIterator tableIterator(
            Map<IntKey, List<TupleHolder>> table,
            TableIteratorType type)
    {
        final Iterator<List<TupleHolder>> tableIt = table.values().iterator();
        //CSOFF
        return new RowIterator()
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
        String desc = String.format("HASH JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, OUTER KEYS: %s, INNER KEYS: %s, PREDICATE: %s)",
                logicalOperator,
                nodeId,
                populating,
                emitEmptyOuterRows,
                null,
                outerHashFunction.toString(),
                innerHashFunction.toString(),
                predicate.toString());
        return desc + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
