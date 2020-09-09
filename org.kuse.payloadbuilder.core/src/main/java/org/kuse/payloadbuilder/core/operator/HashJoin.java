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
    private final ToIntBiFunction<ExecutionContext, Row> outerHashFunction;
    private final ToIntBiFunction<ExecutionContext, Row> innerHashFunction;
    private final BiPredicate<ExecutionContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    HashJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ToIntBiFunction<ExecutionContext, Row> outerHashFunction,
            ToIntBiFunction<ExecutionContext, Row> innerHashFunction,
            BiPredicate<ExecutionContext, Row> predicate,
            RowMerger rowMerger,
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
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(LOGICAL_OPERATOR, logicalOperator),
                entry(POPULATING, populating),
                entry(PREDICATE, predicate),
                entry(OUTER_VALUES, outerHashFunction),
                entry(INNER_VALUES, innerHashFunction));
    }

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
        Map<IntKey, List<Row>> table = hash(context);
        if (table.isEmpty())
        {
            return RowIterator.EMPTY;
        }

        boolean markOuterRows = populating || emitEmptyOuterRows;
        RowIterator probeIterator = probeIterator(context.getRow(), table, context, markOuterRows);

        if (populating)
        {
            // Populate inner rows
            while (probeIterator.hasNext())
            {
                probeIterator.next();
            }

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
                tableIterator(table, TableIteratorType.NON_MATCHED)));
    };

    private Map<IntKey, List<Row>> hash(ExecutionContext context)
    {
        IntKey key = new IntKey();
        Map<IntKey, List<Row>> table = new LinkedHashMap<>();
        RowIterator oi = outer.open(context);
        while (oi.hasNext())
        {
            Row row = oi.next();
            key.key = outerHashFunction.applyAsInt(context, row);
            List<Row> list = table.get(key);
            if (list == null)
            {
                // Start with singleton list
                list = singletonList(row);
                table.put(key, list);
                continue;
            }
            else if (list.size() == 1)
            {
                // Convert to array list
                list = new ArrayList<>(list);
                table.put(key, list);
            }
            list.add(row);
        }
        oi.close();
        return table;
    }

    private RowIterator probeIterator(
            Row contextParent,
            Map<IntKey, List<Row>> table,
            ExecutionContext context,
            boolean markOuterRows)
    {
        final RowIterator ii = inner.open(context);
        return new RowIterator()
        {
            Row next;
            Row currentInner;
            List<Row> outerList;
            int outerIndex;
            IntKey key = new IntKey();

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
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

                        key.key = innerHashFunction.applyAsInt(context, currentInner);
                        List<Row> list = table.get(key);
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

                    Row currentOuter = outerList.get(outerIndex++);
                    currentOuter.setPredicateParent(contextParent);
                    currentInner.setPredicateParent(currentOuter);

                    if (predicate.test(context, currentInner))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, populating);
                        if (markOuterRows)
                        {
                            currentOuter.match = true;
                        }
                    }

                    currentInner.clearPredicateParent();
                    currentOuter.clearPredicateParent();
                }

                return true;
            }
        };
    }

    private RowIterator tableIterator(
            Map<IntKey, List<Row>> table,
            TableIteratorType type)
    {
        final Iterator<List<Row>> tableIt = table.values().iterator();
        return new RowIterator()
        {
            private Row next;
            private List<Row> list;
            private int index;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row result = next;
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

                    Row row = list.get(index++);
                    if ((type == TableIteratorType.MATCHED && row.match)
                        ||
                        type == TableIteratorType.NON_MATCHED && !row.match
                        ||
                        type == TableIteratorType.BOTH)
                    {
                        next = row;
                    }
                    row.match = false;
                }
                return true;
            }
        };
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
    }

    private enum TableIteratorType
    {
        MATCHED,
        NON_MATCHED,
        BOTH;
    }

    @Override
    public int hashCode()
    {
        return 17 + (outer.hashCode() * 37) + (inner.hashCode() * 37);
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
