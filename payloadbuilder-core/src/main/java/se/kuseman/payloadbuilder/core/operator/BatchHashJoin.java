package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.core.DescribeUtils.BATCH_SIZE;
import static se.kuseman.payloadbuilder.core.DescribeUtils.INNER_HASH_TIME;
import static se.kuseman.payloadbuilder.core.DescribeUtils.INNER_VALUES;
import static se.kuseman.payloadbuilder.core.DescribeUtils.LOGICAL_OPERATOR;
import static se.kuseman.payloadbuilder.core.DescribeUtils.OUTER_HASH_TIME;
import static se.kuseman.payloadbuilder.core.DescribeUtils.OUTER_VALUES;
import static se.kuseman.payloadbuilder.core.DescribeUtils.POPULATING;
import static se.kuseman.payloadbuilder.core.DescribeUtils.PREDICATE_TIME;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.NodeData;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.parser.Option;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

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
    private static final int DEFAULT_INNER_CAPACITY = 10;
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final IOrdinalValuesFactory outerValuesFactory;
    private final ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction;
    private final Predicate<ExecutionContext> predicate;
    private final TupleMerger tupleMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final Option batchSizeOption;

    // CSOFF
    BatchHashJoin(
            // CSON
            int nodeId, String logicalOperator, Operator outer, Operator inner, IOrdinalValuesFactory outerValuesFactory, ToIntBiFunction<ExecutionContext, Tuple> innerHashFunction,
            Predicate<ExecutionContext> predicate, TupleMerger tupleMerger, boolean populating, boolean emitEmptyOuterRows, Index innerIndex, Option batchSizeOption)
    {
        super(nodeId);
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerValuesFactory = requireNonNull(outerValuesFactory, "outerValuesExtractor");
        this.innerHashFunction = requireNonNull(innerHashFunction, "innerHashFunction");
        this.predicate = requireNonNull(predicate, "predicate");
        this.tupleMerger = requireNonNull(tupleMerger, "tupleMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.innerIndex = requireNonNull(innerIndex, "innerIndex");
        this.batchSizeOption = batchSizeOption;
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
        return "Batch Hash Join";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        Map<String, Object> result = ofEntries(true, entry(LOGICAL_OPERATOR, logicalOperator), entry(POPULATING, populating),
                entry(BATCH_SIZE, batchSizeOption != null ? batchSizeOption.getValueExpression()
                        .toString()
                        : innerIndex.getBatchSize()),
                entry(PREDICATE, predicate), entry(INDEX, innerIndex), entry(OUTER_VALUES, outerValuesFactory), entry(INNER_VALUES, innerHashFunction));

        if (data != null)
        {
            result.put(INNER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.innerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(OUTER_HASH_TIME, DurationFormatUtils.formatDurationHMS(data.outerHashTime.getTime(TimeUnit.MILLISECONDS)));
            result.put(PREDICATE_TIME, DurationFormatUtils.formatDurationHMS(data.predicateTime.getTime(TimeUnit.MILLISECONDS)));
        }

        return result;
    }

    // CSOFF
    @Override
    // CSON
    public TupleIterator open(IExecutionContext ctx)
    {
        ExecutionContext context = (ExecutionContext) ctx;
        final JoinTuple joinTuple = new JoinTuple(context.getStatementContext()
                .getTuple());
        final Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, Data::new);
        final TupleIterator outerIt = new ATupleIterator(outer.open(context));
        final int batchSize = getBatchSize(context);
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            /** Batched rows */
            private boolean outerTuplesResized;
            private List<TupleHolder> outerTuples;
            private int outerTupleIndex;
            private List<Tuple> innerTuples;
            private int innerRowIndex;

            /** Reference to outer values iterator to verify that implementations of Operator fully uses the index if specified */
            private Iterator<IOrdinalValues> outerValuesIterator;

            /** Table use for hashed inner values */
            private Int2ObjectMap<List<Tuple>> table;

            private boolean addedValuesResized;
            private ObjectSet<IOrdinalValues> addedValues = new ObjectOpenHashSet<>(10);

            private Tuple next;
            private TupleHolder outerTuple;
            private TupleHolder prevOuterTuple;

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
            }

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
                        // Clear tables before processing next batch
                        addedValues.clear();
                        if (table != null)
                        {
                            table.clear();
                        }

                        batchOuterRows();
                        if (outerTuples.isEmpty())
                        {
                            verifyOuterValuesIterator();
                            return false;
                        }

                        // No inner rows was fetched
                        fetchInnerBatch();

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
                            // CSOFF
                            if (populating)
                            // CSON
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
                        innerTuples = table.get(outerTuple.hash);
                        if (innerTuples == null)
                        {
                            // CSOFF
                            if (!populating
                                    && emitEmptyOuterRows)
                            // CSON
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
                    context.getStatementContext()
                            .setTuple(joinTuple);
                    boolean result = predicate.test(context);
                    context.getStatementContext()
                            .setTuple(null);
                    data.predicateTime.suspend();

                    if (result)
                    {
                        next = tupleMerger.merge(outerTuple.tuple, innerRow, populating);

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
                    // Pick prev tuple if any
                    outerTuples = prevOuterTuple != null ? singletonList(prevOuterTuple)
                            : emptyList();
                    prevOuterTuple = null;
                    return;
                }

                joinTuple.setOuter(null);
                int count = 0;
                int size = batchSize > 0 ? batchSize
                        : 100;
                outerTuples = new ArrayList<>(10);

                // Adapt for eventual prev tuple
                if (prevOuterTuple != null)
                {
                    outerTuples.add(prevOuterTuple);
                    prevOuterTuple = null;
                    count = 1;
                }

                // Flag to indicate if the batch is complete
                // then we keep fetching outer tuples until the values
                // changes, this to avoid calling down stream operator
                // multiple times with the same values
                /*
                 * @formatter:off
                  /*
                 * ie.
                 *
                 * outer value
                 * 1
                 * 1             Batch 1
                 * 2
                 * --------
                 * 2
                 * 3             Batch 2
                 * 3
                 *
                 *
                 * Here we should fetch batches like this:
                 *
                 * 1
                 * 1
                 * 2             Batch 1
                 * 2
                 * --------
                 * 3             Batch 2
                 * 3
                 *
                 * @formatter:on
                 */
                boolean batchComplete = false;

                while (outerIt.hasNext())
                {
                    Tuple tuple = outerIt.next();

                    joinTuple.setInner(tuple);
                    IOrdinalValues outerValues = outerValuesFactory.create(context, joinTuple);
                    int hash = outerValues.hashCode();

                    TupleHolder holder = new TupleHolder(tuple, hash);
                    batchComplete = count >= size;

                    if (!addedValuesResized
                            && addedValues.size() >= 10)
                    {
                        ObjectSet<IOrdinalValues> set = addedValues;
                        addedValues = new ObjectOpenHashSet<>(batchSize);
                        addedValues.addAll(set);
                        addedValuesResized = true;
                    }

                    // TODO: If the outer values ordinals is unique (contains index columns, only one needed?)
                    // then this set is not needed since we know that all outer values will provide unique values
                    // If current tuple yielded unique values
                    // use it when opening the down stream operator
                    if (addedValues.add(outerValues))
                    {
                        holder.outerValues = outerValues;
                        // First unique values after the batch was complete, drop out
                        // and use current tuple in the next batch.
                        // This to avoid fetching the same inner tuples multiple times
                        if (batchComplete)
                        {
                            prevOuterTuple = holder;
                            break;
                        }
                    }

                    // If we had more tuples that initial capacity then ensure that capacity is equal to batch size
                    // This to not allocate to large object arrays if the outer tuples was < 10 rows
                    if (!outerTuplesResized
                            && outerTuples.size() >= 10)
                    {
                        ((ArrayList<TupleHolder>) outerTuples).ensureCapacity(batchSize + 1);
                        outerTuplesResized = true;
                    }

                    outerTuples.add(holder);
                    count++;
                }
            }

            private boolean fetchInnerBatch()
            {
                if (table == null)
                {
                    table = new Int2ObjectOpenHashMap<>(outerTuples.size());
                }

                outerValuesIterator = outerTuples.stream()
                        .filter(o -> o.outerValues != null)
                        .map(o -> o.outerValues)
                        .iterator();

                if (!outerValuesIterator.hasNext())
                {
                    return false;
                }

                context.getStatementContext()
                        .setOuterOrdinalValues(outerValuesIterator);
                TupleIterator it = new ATupleIterator(inner.open(context));

                joinTuple.setOuter(null);
                boolean tuplesFetched = false;

                // Hash batch
                while (it.hasNext())
                {
                    tuplesFetched = true;
                    Tuple tuple = it.next();
                    joinTuple.setInner(tuple);
                    data.innerHashTime.resume();
                    try
                    {
                        int hash = innerHashFunction.applyAsInt(context, joinTuple);
                        List<Tuple> list = table.get(hash);
                        if (list == null)
                        {
                            // Start with singleton list
                            list = singletonList(tuple);
                            table.put(hash, list);
                            continue;
                        }
                        else if (list.size() == 1)
                        {
                            // Switch to array list
                            List<Tuple> newList = new ArrayList<>(DEFAULT_INNER_CAPACITY);
                            newList.add(list.get(0));
                            list = newList;
                            table.put(hash, list);
                        }
                        list.add(tuple);
                    }
                    finally
                    {
                        data.innerHashTime.suspend();
                    }
                }
                it.close();

                verifyOuterValuesIterator();
                context.getStatementContext()
                        .setOuterOrdinalValues(null);
                return tuplesFetched;
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
                if (holder.match
                        || emitEmptyOuterRows)
                {
                    next = holder.tuple;
                }

                outerTupleIndex++;
            }

            private void verifyOuterValuesIterator()
            {
                if (outerValuesIterator != null
                        && outerValuesIterator.hasNext())
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
        };
    }

    private int getBatchSize(ExecutionContext context)
    {
        int temp = innerIndex.getBatchSize();
        if (batchSizeOption != null)
        {
            Object obj = batchSizeOption.getValueExpression()
                    .eval(context);
            if (!(obj instanceof Integer)
                    || (Integer) obj < 0)
            {
                throw new OperatorException("Batch size expression " + batchSizeOption.getValueExpression() + " should return a positive Integer. Got: " + obj);
            }
            temp = (int) obj;
        }
        return temp;
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
        if (obj instanceof BatchHashJoin)
        {
            BatchHashJoin that = (BatchHashJoin) obj;
            return nodeId.equals(that.nodeId)
                    && logicalOperator.equals(that.logicalOperator)
                    && outer.equals(that.outer)
                    && inner.equals(that.inner)
                    && outerValuesFactory.equals(that.outerValuesFactory)
                    && innerHashFunction.equals(that.innerHashFunction)
                    && predicate.equals(that.predicate)
                    && tupleMerger.equals(that.tupleMerger)
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
        String description = String.format("BATCH HASH JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, BATCH SIZE: %s, INDEX: %s, OUTER VALUES: %s, INNER VALUES: %s, PREDICATE: %s)", logicalOperator,
                nodeId, populating, emitEmptyOuterRows, batchSizeOption != null ? batchSizeOption.getValueExpression()
                        .toString()
                        : innerIndex.getBatchSize(),
                innerIndex, outerValuesFactory, innerHashFunction, predicate);
        return description + System.lineSeparator() + indentString + outer.toString(indent + 1) + System.lineSeparator() + indentString + inner.toString(indent + 1);
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

    /** Holder for a tuple with properties needed to perform operator logic */
    private static class TupleHolder
    {
        // Non cache key data
        private Tuple tuple;
        private final int hash;

        /** Flag to indicate that this holder should be pushed to downstream operator to be used as index value foundation */
        private IOrdinalValues outerValues;
        /** Flag to indicate if this tuple got a match or not. Used in populating and left joins */
        private boolean match;

        TupleHolder(Tuple tuple, int hash)
        {
            this.tuple = tuple;
            this.hash = hash;
        }
    }
}
