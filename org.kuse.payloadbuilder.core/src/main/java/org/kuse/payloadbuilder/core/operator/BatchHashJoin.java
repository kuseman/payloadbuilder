package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.kuse.payloadbuilder.core.DescribeUtils.BATCH_SIZE;
import static org.kuse.payloadbuilder.core.DescribeUtils.INDEX;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.Option;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * <pre>
 * Special variant of hash join that merges two streams by first batching outer rows
 * And push these down to inner operator that in turn returns inner rows connected to the outer rows
 *
 * Then hashing is performed like a regular hash join
 *
 * from source s
 * inner join article a
 *   on a.art_id = sqrt(s.value)
 *
 * hash (sqrt(s.value))
 * probe a.art_id
 * </pre>
 **/
class BatchHashJoin extends AOperator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final ValuesExtractor outerValuesExtractor;
    private final ValuesExtractor innerValuesExtractor;
    private final Predicate<ExecutionContext> predicate;
    private final TupleMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final int valuesSize;
    private final Option batchSizeOption;

    //CSOFF
    BatchHashJoin(
            //CSON
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ValuesExtractor outerValuesExtractor,
            ValuesExtractor innerValuesExtractor,
            Predicate<ExecutionContext> predicate,
            TupleMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows,
            Index innerIndex,
            Option batchSizeOption)
    {
        super(nodeId);
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerValuesExtractor = requireNonNull(outerValuesExtractor, "outerValuesExtractor");
        this.innerValuesExtractor = requireNonNull(innerValuesExtractor, "innerValuesExtractor");
        this.predicate = requireNonNull(predicate, "predicate");
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.innerIndex = requireNonNull(innerIndex, "innerIndex");
        this.valuesSize = outerValuesExtractor.size();
        this.batchSizeOption = batchSizeOption;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return asList(outer, inner);
    }

    @Override
    public String getName()
    {
        return "Batch Hash Join";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        Data data = context.getOperatorContext().getNodeData(nodeId);
        Map<String, Object> result = ofEntries(true,
                entry(LOGICAL_OPERATOR, logicalOperator),
                entry(POPULATING, populating),
                entry(BATCH_SIZE, batchSizeOption != null ? batchSizeOption.getValueExpression().toString() : innerIndex.getBatchSize()),
                entry(PREDICATE, predicate),
                entry(INDEX, innerIndex),
                entry(OUTER_VALUES, outerValuesExtractor),
                entry(INNER_VALUES, innerValuesExtractor));

        if (data != null)
        {
            result.put(INNER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.innerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(OUTER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.outerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(PREDICATE_TIME, DurationFormatUtils.formatDurationHMS(data.predicateTime.getTime(TimeUnit.MILLISECONDS)));
        }

        return result;
    }

    //CSOFF
    @Override
    //CSON
    public RowIterator open(ExecutionContext context)
    {
        final JoinTuple joinTuple = new JoinTuple(context.getTuple());
        final Data data = context.getOperatorContext().getNodeData(nodeId, Data::new);
        final RowIterator outerIt = outer.open(context);
        final int batchSize = getBatchSize(context);
        //CSOFF
        return new RowIterator()
        //CSON
        {
            /** Batched rows */
            private List<TupleHolder> outerTuples;
            private int outerTupleIndex;
            private List<Tuple> innerTuples;
            private int innerRowIndex;

            /** Reference to outer values iterator to verify that implementations of Operator fully uses the index if specified */
            private Iterator<Object[]> outerValuesIterator;

            /** Table use for hashed inner values */
            private final TIntObjectMap<TableValue> table = new TIntObjectHashMap<>((int) (batchSize * 1.5));

            private Tuple next;
            private TupleHolder outerTuple;

            private final Object[] keyValues = new Object[valuesSize];

            /** Flag used when having populating join or, left join */
            private boolean emitOuterRows;

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

            @Override
            public void close()
            {
                outerIt.close();
            };

            private boolean setNext()
            {
                while (next == null)
                {
                    // Populating mode
                    if (emitOuterRows)
                    {
                        emitOuterRows();
                        continue;
                    }

                    if (outerTuples == null)
                    {
                        batchOuterRows();
                        if (outerTuples.isEmpty())
                        {
                            verifyOuterValuesIterator();
                            return false;
                        }

                        hashInnerBatch();

                        // Start probing
                        continue;
                    }
                    // Probe current outer tuple
                    else if (innerTuples == null)
                    {
                        // We're done
                        if (outerTupleIndex >= outerTuples.size())
                        {
                            // Populating mode, emit outer rows
                            //CSOFF
                            if (populating)
                            //CSON
                            {
                                outerTupleIndex = 0;
                                emitOuterRows = true;
                            }
                            else
                            {
                                clearOuterRows();
                            }
                            continue;
                        }

                        outerTuple = outerTuples.get(outerTupleIndex);
                        TableValue tableValue = table.get(outerTuple.hash);
                        if (tableValue == null)
                        {
                            outerTupleIndex++;
                            continue;
                        }

                        innerTuples = tableValue.getRows();
                        if (innerTuples.isEmpty())
                        {
                            //CSOFF
                            if (!populating && emitEmptyOuterRows)
                            //CSON
                            {
                                next = outerTuple.tuple;
                            }

                            innerTuples = null;
                            outerTupleIndex++;
                            continue;
                        }
                        joinTuple.setOuter(outerTuple.tuple);
                    }
                    else if (innerRowIndex >= innerTuples.size())
                    {
                        outerTupleIndex++;
                        innerTuples = null;
                        innerRowIndex = 0;
                        continue;
                    }

                    Tuple innerRow = innerTuples.get(innerRowIndex++);
                    joinTuple.setInner(innerRow);

                    data.predicateTime.resume();
                    context.setTuple(joinTuple);
                    boolean result = predicate.test(context);
                    context.setTuple(null);
                    data.predicateTime.suspend();

                    if (result)
                    {
                        next = rowMerger.merge(outerTuple.tuple, innerRow, populating);

                        if (populating)
                        {
                            outerTuple.tuple = next;
                            outerTuple.match = true;
                            next = null;
                        }
                    }
                }
                return true;
            }

            /** Batch outer rows and generate outer keys */
            private void batchOuterRows()
            {
                // Stop early to avoid allocation of lists
                if (!outerIt.hasNext())
                {
                    outerTuples = emptyList();
                    return;
                }

                int count = 0;
                int size = batchSize > 0 ? batchSize : 100;
                outerTuples = new ArrayList<>(size);
                while (outerIt.hasNext())
                {
                    Tuple tuple = outerIt.next();
                    outerTuples.add(new TupleHolder(tuple));
                    count++;
                    if (batchSize > 0 && count >= batchSize)
                    {
                        break;
                    }
                }
            }

            private void hashInnerBatch()
            {
                outerValuesIterator = outerValuesIterator(context);
                if (!outerValuesIterator.hasNext())
                {
                    return;
                }

                context.getOperatorContext().setOuterIndexValues(outerValuesIterator);
                RowIterator it = inner.open(context);

                joinTuple.setOuter(null);

                // Hash batch
                while (it.hasNext())
                {
                    Tuple tuple = it.next();
                    joinTuple.setInner(tuple);
                    data.innerHashTime.resume();
                    try
                    {
                        int hash = populateKeyValues(innerValuesExtractor, context, joinTuple);
                        TableValue tableValue = table.get(hash);
                        if (tableValue == null)
                        {
                            // No outer row exists for this inner rows hash, no need to add it
                            continue;
                        }
                        tableValue.addRow(tuple);
                    }
                    finally
                    {
                        data.innerHashTime.suspend();
                    }
                }
                it.close();

                verifyOuterValuesIterator();
                context.getOperatorContext().setOuterIndexValues(null);
            }

            private void emitOuterRows()
            {
                // Complete
                if (outerTupleIndex > outerTuples.size() - 1)
                {
                    clearOuterRows();
                    return;
                }

                TupleHolder holder = outerTuples.get(outerTupleIndex);
                if (holder.match || emitEmptyOuterRows)
                {
                    next = holder.tuple;
                }

                outerTupleIndex++;
            }

            private void verifyOuterValuesIterator()
            {
                if (outerValuesIterator != null && outerValuesIterator.hasNext())
                {
                    throw new IllegalArgumentException("Check implementation of operator with index: " + innerIndex + " not all outer values was processed.");
                }
            }

            private void clearOuterRows()
            {
                outerTuple = null;
                outerTuples = null;
                outerTupleIndex = 0;
                emitOuterRows = false;
            }

            private int populateKeyValues(
                    ValuesExtractor valueExtractor,
                    ExecutionContext context,
                    Tuple tuple)
            {
                valueExtractor.extract(context, tuple, keyValues);
                return hash(keyValues);
            }

            private Iterator<Object[]> outerValuesIterator(ExecutionContext context)
            {
                //CSOFF
                return new Iterator<Object[]>()
                //CSON
                {
                    private int outerRowsIndex;
                    private Object[] nextArray;

                    @Override
                    public boolean hasNext()
                    {
                        return setNext();
                    }

                    @Override
                    public Object[] next()
                    {
                        if (nextArray == null)
                        {
                            throw new NoSuchElementException();
                        }

                        Object[] result = nextArray;
                        nextArray = null;
                        return result;
                    }

                    private boolean setNext()
                    {
                        while (nextArray == null)
                        {
                            if (outerRowsIndex >= outerTuples.size())
                            {
                                return false;
                            }

                            data.outerHashTime.resume();
                            TupleHolder holder = outerTuples.get(outerRowsIndex++);
                            joinTuple.setOuter(null);
                            joinTuple.setInner(holder.tuple);
                            try
                            {
                                int hash = populateKeyValues(outerValuesExtractor, context, joinTuple);
                                // Cannot be null values in keys
                                if (contains(keyValues, null))
                                {
                                    continue;
                                }
                                holder.hash = hash;
                                TableValue tableValue = table.get(holder.hash);
                                if (tableValue == null)
                                {
                                    tableValue = new TableValue();
                                    table.put(holder.hash, tableValue);
                                }

                                if (!tableValue.addInnterValues(keyValues))
                                {
                                    // Value already present, no need to push values to downstream
                                    continue;
                                }
                            }
                            finally
                            {
                                data.outerHashTime.suspend();
                            }

                            nextArray = keyValues;
                        }

                        return true;
                    }
                };
            }
        };
    }

    private int getBatchSize(ExecutionContext context)
    {
        int temp = innerIndex.getBatchSize();
        if (batchSizeOption != null)
        {
            Object obj = batchSizeOption.getValueExpression().eval(context);
            if (!(obj instanceof Integer) || (Integer) obj < 0)
            {
                throw new OperatorException("Batch size expression " + batchSizeOption.getValueExpression() + " should return a positive Integer. Got: " + obj);
            }
            temp = (int) obj;
        }
        return temp;
    }

    private int hash(Object[] values)
    {
        int result = ObjectUtils.HASH_CONSTANT;

        int length = values.length;
        for (int i = 0; i < length; i++)
        {
            Object value = values[i];
            result = result * ObjectUtils.HASH_MULTIPLIER + ObjectUtils.hash(value);
        }
        return result;
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
        if (obj instanceof BatchHashJoin)
        {
            BatchHashJoin that = (BatchHashJoin) obj;
            return nodeId == that.nodeId
                && logicalOperator.equals(that.logicalOperator)
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && outerValuesExtractor.equals(that.outerValuesExtractor)
                && innerValuesExtractor.equals(that.innerValuesExtractor)
                && predicate.equals(that.predicate)
                && rowMerger.equals(that.rowMerger)
                && populating == that.populating
                && emitEmptyOuterRows == that.emitEmptyOuterRows
                && innerIndex.equals(that.innerIndex)
                && Objects.equals(batchSizeOption, that.batchSizeOption);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format(
                "BATCH HASH JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, BATCH SIZE: %s, INDEX: %s, OUTER VALUES: %s, INNER VALUES: %s, PREDICATE: %s)",
                logicalOperator,
                nodeId,
                populating,
                emitEmptyOuterRows,
                batchSizeOption != null ? batchSizeOption.getValueExpression().toString() : innerIndex.getBatchSize(),
                innerIndex,
                outerValuesExtractor,
                innerValuesExtractor,
                predicate);
        return description + System.lineSeparator()
            + indentString + outer.toString(indent + 1) + System.lineSeparator()
            + indentString + inner.toString(indent + 1);
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

    /** Tuple holder */
    private static class TupleHolder
    {
        private Tuple tuple;
        private int hash;
        private boolean match;

        TupleHolder(Tuple tuple)
        {
            this.tuple = tuple;
        }
    }

    /** Value used in table */
    private static class TableValue
    {
        private List<Tuple> tuples;
        private List<Object[]> values;

        /** Check if provided key exists in values */
        private boolean contains(Object[] keyValues)
        {
            int size = values.size();
            for (int i = 0; i < size; i++)
            {
                if (Arrays.equals(values.get(i), keyValues))
                {
                    return true;
                }
            }
            return false;
        }

        public void addRow(Tuple tuple)
        {
            if (tuples == null)
            {
                tuples = new ArrayList<>();
            }

            tuples.add(tuple);
        }

        public List<Tuple> getRows()
        {
            return tuples != null ? tuples : emptyList();
        }

        /**
         * Add inner values array
         *
         * @return True if array was added
         */
        boolean addInnterValues(Object[] keyValues)
        {
            if (values == null)
            {
                values = new ArrayList<>();
            }
            else if (contains(keyValues))
            {
                return false;
            }

            values.add(Arrays.copyOf(keyValues, keyValues.length));
            return true;
        }
    }
}
