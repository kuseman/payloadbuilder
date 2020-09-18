/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.kuse.payloadbuilder.core.DescribeUtils.BATCH_SIZE;
import static org.kuse.payloadbuilder.core.DescribeUtils.INDEX;
import static org.kuse.payloadbuilder.core.DescribeUtils.INNER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.LOGICAL_OPERATOR;
import static org.kuse.payloadbuilder.core.DescribeUtils.OUTER_VALUES;
import static org.kuse.payloadbuilder.core.DescribeUtils.POPULATING;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Option;

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
    private final BiPredicate<ExecutionContext, Tuple> predicate;
    private final TupleMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final int valuesSize;
    private final Option batchSizeOption;

    /* Statistics */
    private int executionCount;

    BatchHashJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ValuesExtractor outerValuesExtractor,
            ValuesExtractor innerValuesExtractor,
            BiPredicate<ExecutionContext, Tuple> predicate,
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
        this.valuesSize = innerIndex.getColumns().size();
        this.batchSizeOption = batchSizeOption;
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(outer, inner);
    }

    @Override
    public String getName()
    {
        return "Batch Hash Join";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(LOGICAL_OPERATOR, logicalOperator),
                entry(POPULATING, populating),
                entry(BATCH_SIZE, batchSizeOption != null ? batchSizeOption.getValueExpression().toString() : innerIndex.getBatchSize()),
                entry(PREDICATE, predicate),
                entry(INDEX, innerIndex),
                entry(OUTER_VALUES, outerValuesExtractor),
                entry(INNER_VALUES, innerValuesExtractor));
    }

    class Data extends NodeData
    {
        AtomicLong time = new AtomicLong();
        long innerHashTime;
        long outerHashTime;

        @Override
        public String toString()
        {
            return "Time (" + innerIndex.toString() + ", innerTime: " + innerHashTime + ", outerTime: " + outerHashTime + "): " + DurationFormatUtils.formatDurationHMS(time.get());
        }
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        final Tuple contextOuter = context.getTuple();
        final JoinTuple joinTuple = new JoinTuple();
        joinTuple.setContextOuter(contextOuter);
        Data data = context.getOperatorContext().getNodeData(nodeId, () -> new Data());
        executionCount++;
        final RowIterator outerIt = outer.open(context);
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
        final StopWatch sw = new StopWatch();
        final int batchSize = temp;
        return new RowIterator()
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
                if (sw.isStopped())
                {
                    sw.start();
                }
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
                            sw.stop();
                            data.time.addAndGet(sw.getTime());
                            sw.reset();

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
                            if (populating)
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
                            if (!populating && emitEmptyOuterRows)
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

                    if (predicate.test(context, joinTuple))
                    {
                        sw1.start();

                        next = rowMerger.merge(outerTuple.tuple, innerRow, populating, nodeId);
                        sw1.stop();
                        data.innerHashTime += sw1.getTime();
                        sw1.reset();

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

            StopWatch sw1 = new StopWatch();

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

                // Hash batch
                while (it.hasNext())
                {
                    Tuple tuple = it.next();
                    int hash = populateKeyValues(innerValuesExtractor, context, tuple);
                    TableValue tableValue = table.get(hash);
                    if (tableValue == null)
                    {
                        // No outer row exists for this inner rows hash, no need to add it
                        continue;
                    }
                    tableValue.addRow(tuple);
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
                return new Iterator<Object[]>()
                {
                    private int outerRowsIndex = 0;
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

                            TupleHolder holder = outerTuples.get(outerRowsIndex++);
                            int hash = populateKeyValues(outerValuesExtractor, context, holder.tuple);
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

                            nextArray = keyValues;
                        }

                        return true;
                    }
                };
            }
        };
    }

    private int hash(Object[] values)
    {
        int result = 1;

        int length = values.length;
        for (int i = 0; i < length; i++)
        {
            Object value = values[i];

            // If value is string and is digits, use the intvalue as
            // hash instead of string to be able to compare ints and strings
            // on left/right side of join
            if (value instanceof String && NumberUtils.isDigits((String) value))
            {
                value = Integer.parseInt((String) value);
            }

            result = 31 * result + (value == null ? 0 : value.hashCode());
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * outer.hashCode() +
            37 * inner.hashCode();
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
                "BATCH HASH JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, BATCH SIZE: %s, INDEX: %s, OUTER VALUES: %s, INNER VALUES: %s, PREDICATE: %s)",
                logicalOperator,
                nodeId,
                populating,
                emitEmptyOuterRows,
                executionCount,
                batchSizeOption != null ? batchSizeOption.getValueExpression().toString() : innerIndex.getBatchSize(),
                innerIndex,
                outerValuesExtractor,
                innerValuesExtractor,
                predicate);
        return description + System.lineSeparator()
            + indentString + outer.toString(indent + 1) + System.lineSeparator()
            + indentString + inner.toString(indent + 1);
    }

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
