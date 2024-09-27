package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.ITupleVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.SelectedTupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

/** Hash match physical plan. Used in left/inner joins where we have an equi condition. */
public class HashMatch implements IPhysicalPlan
{
    /** If inner input size is unkown and outer is known and above this threshold, we take a chance and hash the inner */
    private static final int OUTER_ROW_THRESHOLD = 10_000;

    private final int nodeId;
    private final IPhysicalPlan outer;
    private final IPhysicalPlan inner;
    private final List<IExpression> outerHashFunction;
    private final List<IExpression> innerHashFunction;

    /** Join predicate */
    private final BiFunction<TupleVector, IExecutionContext, ValueVector> condition;
    /** The alias if this is a populating join */
    private final String populateAlias;
    /** Should empty outer rows be emited. True when having left join/outer apply. */
    private final boolean emitEmptyOuterRows;
    /**
     * Should outer reference be pushed into context before executing inner. This is used when having a seek predicate
     */
    private final boolean pushOuterReference;

    private final boolean isAsteriskSchema;
    private final boolean isAsteriskInnerSchema;
    private final Schema schema;
    private final Schema cartesianSchema;

    //@formatter:off
    public HashMatch(
            int nodeId,
            IPhysicalPlan outer,
            IPhysicalPlan inner,
            List<IExpression> outerHashFunction,
            List<IExpression> innerHashFunction,
            BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            String populateAlias,
            boolean emitEmptyOuterRows,
            boolean pushOuterReference)
    //@formatter:on
    {
        this.nodeId = nodeId;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "innter");
        this.outerHashFunction = requireNonNull(outerHashFunction, "outerHashFunction");
        this.innerHashFunction = requireNonNull(innerHashFunction, "innerHashFunction");
        this.condition = requireNonNull(condition, "condition");
        this.populateAlias = populateAlias;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.pushOuterReference = pushOuterReference;

        if (outerHashFunction.size() != innerHashFunction.size())
        {
            throw new IllegalArgumentException("Hash functions must equal in size");
        }

        this.schema = getSchema();
        this.cartesianSchema = getSchema(true);
        this.isAsteriskSchema = SchemaUtils.isAsterisk(schema);
        this.isAsteriskInnerSchema = SchemaUtils.isAsterisk(inner.getSchema());
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Hash Match";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        HashNodeData nodeData = context.getStatementContext()
                .getNodeData(nodeId);
        if (nodeData != null)
        {
            properties.put("Hash Time", DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(nodeData.hashTime, TimeUnit.NANOSECONDS)));
            properties.put("Probe Time", DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(nodeData.probeTime, TimeUnit.NANOSECONDS)));
        }

        properties.put(IDatasource.PREDICATE, condition.toString());
        properties.put("Outer Hash Keys", outerHashFunction.toString());
        properties.put("Inner Hash Keys", innerHashFunction.toString());
        properties.put("Populate", populateAlias != null);
        properties.put("Logical Operator", emitEmptyOuterRows ? "LEFT JOIN"
                : "INNER JOIN");

        return properties;
    }

    @Override
    public Schema getSchema()
    {
        return getSchema(false);
    }

    private Schema getSchema(boolean cartesian)
    {
        Schema outerSchema = outer.getSchema();
        Schema innerSchema = inner.getSchema();
        return SchemaUtils.joinSchema(outerSchema, innerSchema, cartesian ? null
                : populateAlias);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        /*
         * Adaptive join - index seek
         * 
         * TupleIterator outer = outer.execute(context);
         * 
         * TupleVector nextOuter = outer.next();
         * 
         * ((ExecutionContext) context).getStatementContext().setIndexSeekTupleVector(nextOuter);
         * 
         * TupleIterator inner = inner.execute(context);
         * 
         */
        TupleIterator outerIt = outer.execute(context);
        if (!outerIt.hasNext())
        {
            outerIt.close();
            return TupleIterator.EMPTY;
        }

        TupleVector outerReference = outerIt.next();
        if (outerReference.getRowCount() == 0)
        {
            outerIt.close();
            return TupleIterator.EMPTY;
        }

        if (pushOuterReference)
        {
            ((ExecutionContext) context).getStatementContext()
                    .setIndexSeekTupleVector(outerReference);
        }

        TupleIterator innerIt = inner.execute(context);
        return createIterator(context, outerIt, innerIt, outerReference);
    }

    /** States of hash match iterator */
    private enum IteratorState
    {
        /** Start phase. Initalize iterator, decide which side to hash etc. */
        Start,

        /** Hash outer stream. */
        HashOuter,

        /** Hash inner stream */
        HashInner,

        /** Probe inner stream */
        ProbeInner,

        /** Probe outer stream */
        ProbeOuter,

        /** Move to next batch. Create non matching vectors on left join etc. */
        NextBatch,

        /** End phase. Close streams etc. */
        End
    }

    TupleIterator createIterator(IExecutionContext context, final TupleIterator topOuterIt, final TupleIterator topInnerIt, TupleVector currentOuter)
    {
        final HashNodeData nodeData = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new HashNodeData());

        return new TupleIterator()
        {
            private final int hashSize = innerHashFunction.size();
            private TupleVector next;
            private TupleVector nonMatchedOuterNext;

            /** Filter that holds matches for probed rows. Used in inner hash mode */
            private BitSet outerMatchedFilter = null;
            /** List with bitset of populated matches. Used in inner hash mode. */
            private List<BitSet> innerPopulateMatches;

            /** Start with the plan schema as inner */
            private Schema innerSchema = inner.getSchema();
            private boolean innerSchemaAsterisk = isAsteriskInnerSchema;
            private Schema outerSchema;

            private ReplicatedTupleVector replicatedTupleVectorOuter = new ReplicatedTupleVector(true);
            private ReplicatedTupleVector replicatedTupleVectorInner = new ReplicatedTupleVector(false);

            private Int2ObjectMap<HashValue> table;
            private boolean outerIsHash;

            private TupleIterator outerIt = topOuterIt;
            private TupleIterator innerIt = topInnerIt;

            /** Outer vector that is extracted to be pushed into context when opening the inner. Only applicable when inner is hashed. */
            private TupleVector outerReference = currentOuter;
            /**
             * The types that should be used when taking the hash of expressions. We need to use the same type for both sides. For example if we join on "1 = '1'" this should yield a match since those
             * values are equal when the string is promoted and hence should be joinable but their hash codes will be different.
             */
            private final Column.Type[] hashFunctionTypes = new Column.Type[outerHashFunction.size()];
            /**
             * List that contains vectors that didn't get any values hashed. Is used when emitting empty outer rows where we don't have entries in table
             */
            private List<TupleVector> emptyVectors;
            private int emptyVectorsIndex;

            private IteratorState state = IteratorState.Start;

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector next = this.next;
                this.next = null;
                return next;
            }

            @Override
            public void close()
            {
                if (innerIt != null)
                {
                    innerIt.close();
                }
                if (outerIt != null)
                {
                    outerIt.close();
                }
            }

            @Override
            public boolean hasNext()
            {
                if (next != null)
                {
                    return true;
                }
                return setNext();
            }

            private boolean setNext()
            {
                /* Traverse the state machine until we have a result */
                while (next == null)
                {
                    if (context.getSession()
                            .abortQuery())
                    {
                        return false;
                    }

                    long time = System.nanoTime();
                    switch (state)
                    {
                        case End:
                            return false;
                        case HashInner:
                            hashInner();
                            nodeData.hashTime += System.nanoTime() - time;
                            break;
                        case HashOuter:
                            hashOuter();
                            nodeData.hashTime += System.nanoTime() - time;
                            break;
                        case NextBatch:
                            nextBatch();
                            break;
                        case ProbeInner:
                            probeInner();
                            nodeData.probeTime += System.nanoTime() - time;
                            break;
                        case ProbeOuter:
                            probeOuter();
                            nodeData.probeTime += System.nanoTime() - time;
                            break;
                        case Start:
                            start();
                            break;
                        default:
                            throw new IllegalStateException("Unknown iterator state: " + state);

                    }
                }
                return next != null;
            }

            /** Handle start state */
            private void start()
            {
                /* Find out which side to hash/probe */
                int outerRowCount = outerIt.estimatedRowCount();

                // If we have indexed join then we use the outer references as row count
                if (pushOuterReference)
                {
                    outerRowCount = outerReference.getRowCount();
                }

                // Hash inner if inner is less than outer
                // or inner is unkown and outer goes above a thres hold
                // then we assume that inner is lower
                // TODO: force option

                // Clear the types before each hash
                Arrays.fill(hashFunctionTypes, null);

                int innerRowCount = innerIt.estimatedRowCount();
                if ((innerRowCount >= 0
                        && innerRowCount < outerRowCount)
                        || (innerRowCount < 0
                                && outerRowCount > OUTER_ROW_THRESHOLD))
                {
                    state = IteratorState.HashInner;
                    outerIsHash = false;
                }
                else
                {
                    state = IteratorState.HashOuter;
                    outerIsHash = true;
                }
            }

            /** Handle next batch state */
            private void nextBatch()
            {
                if (emitEmptyOuterRows)
                {
                    // Pick non matched vector generated in probe state if any
                    if (nonMatchedOuterNext != null)
                    {
                        next = nonMatchedOuterNext;
                        nonMatchedOuterNext = null;
                    }
                    // Else stream out empty vectors or non matched vector in outer hash mode
                    else if (outerIsHash
                            && (innerIt == null
                                    || !innerIt.hasNext()))
                    {
                        next = buildNonMatchedOuterVectorOuterHashMode();
                    }

                    // Stream out non matched tuples until there are no more
                    if (next != null)
                    {
                        return;
                    }
                }

                // We still have inner batches left => continue probing inner
                if (innerIt != null
                        && innerIt.hasNext())
                {
                    state = IteratorState.ProbeInner;
                    return;
                }
                // We still have outer batches left => continue probing outer
                else if (!pushOuterReference
                        && outerIt != null
                        && outerIt.hasNext())
                {
                    state = IteratorState.ProbeOuter;
                    return;
                }

                // hash inner => probe outer
                // or hash outer without push outer reference
                if (outerIt == null
                        || !outerIt.hasNext())
                {
                    state = IteratorState.End;
                    return;
                }

                if (pushOuterReference)
                {
                    outerReference = outerIt.next();
                    ((ExecutionContext) context).getStatementContext()
                            .setIndexSeekTupleVector(outerReference);

                    if (innerIt != null)
                    {
                        innerIt.close();
                    }

                    // Fetch next inner batch
                    innerIt = inner.execute(context);
                    // Start over
                    state = IteratorState.Start;
                }
                else
                {
                    state = IteratorState.End;
                }
            }

            /** Handle hash inner state */
            private void hashInner()
            {
                if (table != null)
                {
                    table.clear();
                }
                int vectorId = 0;
                try
                {
                    while (innerIt != null
                            && innerIt.hasNext())
                    {
                        // Populate then we need the whole concated inner
                        TupleVector vector = populateAlias != null ? PlanUtils.concat(context, innerIt)
                                : innerIt.next();

                        // Clear reference, not needed any more
                        if (populateAlias != null)
                        {
                            innerIt = null;
                        }

                        if (vector.getRowCount() == 0)
                        {
                            continue;
                        }

                        // Set schemas from runtime vectors
                        if (innerSchemaAsterisk)
                        {
                            innerSchema = vector.getSchema();
                            innerSchemaAsterisk = false;
                        }

                        TupleVector evalVector = !isAsteriskSchema ? new InnerTupleVector(cartesianSchema, outer.getSchema()
                                .getSize(), vector)
                                : vector;
                        addToTable(vectorId++, evalVector, vector, null, innerHashFunction);
                        if (populateAlias != null)
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    // When populate it's already closed above in concat
                    if (innerIt != null)
                    {
                        innerIt.close();
                    }
                    // Clear this reference, it's not needed anymore
                    innerIt = null;
                }

                state = IteratorState.ProbeOuter;
            }

            /** Handle hash outer state */
            private void hashOuter()
            {
                if (table != null)
                {
                    table.clear();
                }
                emptyVectors = null;
                emptyVectorsIndex = 0;
                int vectorId = 0;
                try
                {
                    do
                    {
                        TupleVector vector;
                        if (outerReference != null)
                        {
                            vector = outerReference;
                            // If we don't have indexed join then we hash the whole outer stream and
                            // not vector by vector
                            if (!pushOuterReference)
                            {
                                outerReference = null;
                            }
                        }
                        else
                        {
                            vector = outerIt.next();
                        }

                        if (vector.getRowCount() == 0)
                        {
                            continue;
                        }

                        // One matched set per vector
                        BitSet matchedSet = new BitSet();
                        int addedSize = addToTable(vectorId++, vector, vector, matchedSet, outerHashFunction);
                        if (addedSize <= 0
                                && emitEmptyOuterRows)
                        {
                            if (emptyVectors == null)
                            {
                                emptyVectors = List.of(vector);
                            }
                            else if (emptyVectors.size() == 1)
                            {
                                List<TupleVector> tmp = new ArrayList<>();
                                tmp.add(emptyVectors.get(0));
                                tmp.add(vector);
                                emptyVectors = tmp;
                            }
                            else
                            {
                                emptyVectors.add(vector);
                            }
                        }
                        // One vector at a time when using index
                        if (pushOuterReference)
                        {
                            break;
                        }
                    } while (outerIt.hasNext());
                }
                finally
                {
                    if (!pushOuterReference)
                    {
                        outerIt.close();
                        // Clear this reference, it's not needed anymore
                        outerIt = null;
                    }
                }

                state = IteratorState.ProbeInner;
            }

            /** Handle probe inner state */
            private void probeInner()
            {
                // No inner vectors continue with next batch
                if (!innerIt.hasNext())
                {
                    state = IteratorState.NextBatch;
                    return;
                }

                TupleVector inner;
                if (populateAlias != null)
                {
                    inner = innerSchemaAsterisk ? PlanUtils.concat(context, innerIt)
                            : PlanUtils.chain(context, innerIt);
                    // Stream is closed and not used any more
                    innerIt = null;
                }
                else
                {
                    inner = innerIt.next();
                }

                // Set schemas from runtime vectors
                if (innerSchemaAsterisk)
                {
                    // Don't overwrite the schema with an empty one
                    innerSchema = inner.getSchema()
                            .getSize() == 0 ? innerSchema
                                    : inner.getSchema();
                    innerSchemaAsterisk = false;
                }

                if (inner.getRowCount() == 0
                        || MapUtils.isEmpty(table))
                {
                    state = IteratorState.NextBatch;
                    return;
                }

                TupleVector evalVector = !isAsteriskSchema ? new InnerTupleVector(cartesianSchema, outer.getSchema()
                        .getSize(), inner)
                        : inner;

                ValueVector[] vectors = new ValueVector[hashSize];
                for (int i = 0; i < hashSize; i++)
                {
                    vectors[i] = innerHashFunction.get(i)
                            .eval(evalVector, context);
                }

                int rowCount = inner.getRowCount();
                ITupleVectorBuilder builder = null;

                rows: for (int probeRow = 0; probeRow < rowCount; probeRow++)
                {
                    for (int j = 0; j < hashSize; j++)
                    {
                        // Any null value in hash keys and we can skip it since
                        // there can never be any matches
                        if (vectors[j].isNull(probeRow))
                        {
                            continue rows;
                        }
                    }

                    int hash = VectorUtils.hash(vectors, hashFunctionTypes, probeRow);
                    HashValue value = table.get(hash);

                    // No matches for current row
                    if (value == null)
                    {
                        continue;
                    }

                    int bucketSize = value.buckets.size();
                    for (int b = 0; b < bucketSize; b++)
                    {
                        HashValueBucket bucket = value.buckets.get(b);

                        replicatedTupleVectorOuter.init(bucket.vector, inner, bucket, probeRow, isAsteriskSchema, cartesianSchema);

                        // Matches between outer / inner
                        ValueVector filter = condition.apply(replicatedTupleVectorOuter, context);

                        int cardinality = filter.getCardinality();
                        if (cardinality > 0)
                        {
                            bucket.markMatch(filter);
                            if (populateAlias == null)
                            {
                                if (builder == null)
                                {
                                    int resultSize = estimateBufferSize(bucket.vector.getRowCount(), rowCount, cardinality);
                                    builder = context.getVectorFactory()
                                            .getTupleVectorBuilder(resultSize);
                                }
                                builder.append(replicatedTupleVectorOuter, filter);
                            }
                            else
                            {
                                bucket.markPopulatedMatchesOuterHash(filter, probeRow);
                            }
                        }
                    }
                }

                TupleVector result = null;
                if (builder != null)
                {
                    result = builder.build();
                }
                // Append populated matches
                else if (populateAlias != null)
                {
                    result = buildPopulatedVectorOuterHashMode(inner);
                }

                next = result;
                state = IteratorState.NextBatch;
            }

            /** Handle probe outer state */
            private void probeOuter()
            {
                /*
                 * If there wasn't any entires in the tabla that means all inner rows was null etc. Start to stream out empty outer rows
                 */
                if (MapUtils.isEmpty(table))
                {
                    // Inner join or no more outer vectors => we're done
                    if (!emitEmptyOuterRows
                            || (outerReference == null
                                    && !outerIt.hasNext()))
                    {
                        state = IteratorState.End;
                        return;
                    }

                    if (outerReference != null)
                    {
                        next = buildNonMatchedOuterVectorEmptyTable(outerReference);
                        outerReference = null;
                    }
                    else
                    {
                        next = buildNonMatchedOuterVectorEmptyTable(outerIt.next());
                    }

                    state = IteratorState.NextBatch;
                    return;
                }

                TupleVector outer = outerReference != null ? outerReference
                        : outerIt.next();

                // Clear the reference when used
                if (outerReference != null)
                {
                    outerReference = null;
                }

                // Set schemas
                if (outerSchema == null)
                {
                    outerSchema = outer.getSchema();
                }

                ValueVector[] vectors = new ValueVector[hashSize];
                for (int i = 0; i < hashSize; i++)
                {
                    vectors[i] = outerHashFunction.get(i)
                            .eval(outer, context);
                }

                int rowCount = outer.getRowCount();
                ITupleVectorBuilder builder = null;

                if (innerPopulateMatches != null)
                {
                    int s = innerPopulateMatches.size();
                    for (int i = 0; i < s; i++)
                    {
                        BitSet bitSet = innerPopulateMatches.get(i);
                        if (bitSet != null)
                        {
                            bitSet.clear();
                        }
                    }
                }

                if (outerMatchedFilter != null)
                {
                    outerMatchedFilter.clear();
                }

                rows: for (int i = 0; i < rowCount; i++)
                {
                    for (int j = 0; j < hashSize; j++)
                    {
                        // Any null value in hash keys and we can skip it since
                        // there can never be any matches
                        if (vectors[j].isNull(i))
                        {
                            continue rows;
                        }
                    }

                    int hash = VectorUtils.hash(vectors, hashFunctionTypes, i);
                    HashValue value = table.get(hash);

                    // No matches for current row
                    if (value == null)
                    {
                        continue;
                    }

                    int bucketSize = value.buckets.size();
                    for (int b = 0; b < bucketSize; b++)
                    {
                        HashValueBucket bucket = value.buckets.get(b);

                        replicatedTupleVectorInner.init(outer, bucket.vector, bucket, i, isAsteriskSchema, cartesianSchema);

                        // Matches between outer / inner
                        ValueVector filter = condition.apply(replicatedTupleVectorInner, context);

                        int cardinality = filter.getCardinality();
                        if (cardinality > 0)
                        {
                            if (emitEmptyOuterRows)
                            {
                                if (outerMatchedFilter == null)
                                {
                                    outerMatchedFilter = new BitSet(rowCount);
                                }

                                outerMatchedFilter.set(i);
                            }
                            if (populateAlias == null)
                            {
                                if (builder == null)
                                {
                                    int resultSize = estimateBufferSize(bucket.vector.getRowCount(), rowCount, cardinality);
                                    builder = context.getVectorFactory()
                                            .getTupleVectorBuilder(resultSize);
                                }
                                builder.append(replicatedTupleVectorInner, filter);
                            }
                            else
                            {
                                if (innerPopulateMatches == null)
                                {
                                    innerPopulateMatches = new ArrayList<>(Collections.nCopies(rowCount, null));
                                }
                                HashValueBucket.markPopulatedMatchesInnerHash(bucket, innerPopulateMatches, filter, i);
                            }
                        }
                    }
                }

                TupleVector result = null;
                if (builder != null)
                {
                    result = builder.build();
                }
                // Append populated matches
                else if (populateAlias != null
                        && innerPopulateMatches != null)
                {
                    result = buildPopulatedVectorInnerHashMode(outer, innerPopulateMatches);
                }

                // Outer is probed then we can build a non matched vector from current probed vector
                if (emitEmptyOuterRows)
                {
                    TupleVector nonMatchedVector = buildNonMatchedOuterVectorInnerHashMode(outer, outerMatchedFilter, rowCount);
                    // If there was no joined result, set the non matched result to next
                    // to skip one iteration in loop
                    if (result == null)
                    {
                        result = nonMatchedVector;
                    }
                    else
                    {
                        nonMatchedOuterNext = nonMatchedVector;
                    }
                }

                next = result;
                state = IteratorState.NextBatch;
            }

            private int estimateBufferSize(int hashTupleVectorRowCount, int probeRowCount, int cardinality)
            {
                // Try to estimate a good size of result vector
                // max of:
                // - probe row count times current match cardinalty
                // - a tenth size of the cartesian between probe and hash vectors
                int cardinalitySize = probeRowCount * cardinality;
                int estimateSize = (probeRowCount * hashTupleVectorRowCount) / 10;
                return Math.min(cardinalitySize, estimateSize);
            }

            private int addToTable(int vectorId, TupleVector evalVector, TupleVector tableVector, BitSet matchedSet, List<IExpression> hashFunction)
            {
                int rowCount = evalVector.getRowCount();
                ValueVector[] vectors = new ValueVector[hashSize];
                // Evaluate hash function for current tuple
                for (int i = 0; i < hashSize; i++)
                {
                    vectors[i] = hashFunction.get(i)
                            .eval(evalVector, context);

                    // Set hash type
                    if (hashFunctionTypes[i] == null)
                    {
                        Column.Type type = vectors[i].type()
                                .getType();

                        // If any then try to resolve which type this and set the real type
                        // this to force the probing side to hash with equal type else we won't get any hits
                        // when types differs but they will equal in a comparison
                        if (type == Column.Type.Any)
                        {
                            type = VectorUtils.getAnyType(vectors[i]);
                        }

                        hashFunctionTypes[i] = type;
                    }

                }

                int rowsAdded = 0;
                // Add rows to table
                rows: for (int i = 0; i < rowCount; i++)
                {
                    for (int j = 0; j < hashSize; j++)
                    {
                        if (vectors[j].isNull(i))
                        {
                            continue rows;
                        }
                    }

                    if (table == null)
                    {
                        table = new Int2ObjectOpenHashMap<HashValue>();
                    }

                    int h = VectorUtils.hash(vectors, hashFunctionTypes, i);
                    table.computeIfAbsent(h, k -> new HashValue())
                            .add(vectorId, tableVector, matchedSet, i);
                    rowsAdded++;
                }
                return rowsAdded;
            }

            /**
             * Builds a populate tuple vector. Used in outer hash mode.
             */
            private TupleVector buildPopulatedVectorOuterHashMode(TupleVector inner)
            {
                ITupleVectorBuilder builder = null;
                List<TupleVector> innerVectors = new ArrayList<>();
                for (HashValue value : table.values())
                {
                    for (HashValueBucket bucket : value.buckets)
                    {
                        if (bucket.populatedMatches == null)
                        {
                            continue;
                        }

                        builder = appendPopulatedVector(bucket, inner, innerVectors, bucket.populatedMatches, builder);
                    }
                }

                if (builder != null)
                {
                    return builder.build();
                }
                return null;
            }

            /**
             * Builds a populate tuple vector. Used in inner hash mode.
             */
            private TupleVector buildPopulatedVectorInnerHashMode(TupleVector outer, List<BitSet> populatedMatches)
            {
                ITupleVectorBuilder builder = null;
                List<TupleVector> innerVectors = new ArrayList<>();
                // We only have one inner vector in inner hash mode so pick the first one
                // they are all the same
                TupleVector inner = table.values()
                        .iterator()
                        .next().topBucket.vector;
                builder = appendPopulatedVector(outer, inner, innerVectors, populatedMatches, builder);
                if (builder != null)
                {
                    return builder.build();
                }
                return null;
            }

            private ITupleVectorBuilder appendPopulatedVector(TupleVector outer, TupleVector inner, List<TupleVector> innerVectors, List<BitSet> populatedMatches, ITupleVectorBuilder builder)
            {
                int rowCount = inner.getRowCount();
                // Create selected tuple vectors for each outer bitset
                BitSet filter = new BitSet();
                boolean allNull = true;
                innerVectors.clear();
                int index = 0;
                for (BitSet bitSet : populatedMatches)
                {
                    if (bitSet == null)
                    {
                        innerVectors.add(null);
                    }
                    else
                    {
                        filter.set(index, true);
                        allNull = false;

                        TupleVector currentInner = SelectedTupleVector.select(inner, VectorUtils.convertToSelectionVector(rowCount, bitSet));
                        innerVectors.add(currentInner);
                    }
                    index++;
                }

                if (allNull)
                {
                    return null;
                }

                int outerSize = outer.getSchema()
                        .getSize();
                Schema schema = SchemaUtils.joinSchema(outer.getSchema(), inner.getSchema(), populateAlias);
                TupleVector vectorToAppend = new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return outer.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        if (column < outerSize)
                        {
                            return outer.getColumn(column);
                        }

                        return new ValueVector()
                        {
                            @Override
                            public ResolvedType type()
                            {
                                return ResolvedType.table(innerSchema);
                            }

                            @Override
                            public int size()
                            {
                                return outer.getRowCount();
                            }

                            @Override
                            public boolean isNull(int row)
                            {
                                return innerVectors.get(row) == null;
                            }

                            @Override
                            public TupleVector getTable(int row)
                            {
                                return innerVectors.get(row);
                            }
                        };
                    }
                };

                if (builder == null)
                {
                    builder = context.getVectorFactory()
                            .getTupleVectorBuilder(table.size());
                }
                builder.append(vectorToAppend, filter);
                return builder;
            }

            private TupleVector createNonMatchedOuterVector(TupleVector outer)
            {
                Schema schema = getSchema(outer.getSchema(), innerSchema);
                int outerSize = outer.getSchema()
                        .getSize();
                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return outer.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        if (column < outerSize)
                        {
                            return outer.getColumn(column);
                        }
                        ResolvedType type = schema.getColumns()
                                .get(column)
                                .getType();
                        return ValueVector.literalNull(type, outer.getRowCount());
                    }
                };
            }

            /** Build non matching outer vector. Used in outer hash mode */
            private TupleVector buildNonMatchedOuterVectorOuterHashMode()
            {
                // If we have vectors that yielded no table entries stream those first
                if (emptyVectors != null)
                {
                    TupleVector vector = emptyVectors.get(emptyVectorsIndex++);
                    if (emptyVectorsIndex >= emptyVectors.size())
                    {
                        emptyVectors = null;
                        emptyVectorsIndex = 0;
                    }
                    return createNonMatchedOuterVector(vector);
                }

                if (MapUtils.isEmpty(table))
                {
                    return null;
                }

                IntSet seenVectors = new IntArraySet();

                ITupleVectorBuilder nonMatchedBuilder = null;
                for (HashValue value : table.values())
                {
                    for (HashValueBucket bucket : value.buckets)
                    {
                        // Vector already processed
                        if (!seenVectors.add(bucket.vectorId))
                        {
                            continue;
                        }

                        // Flip the matches to get non matches
                        bucket.matchedSet.flip(0, bucket.vector.getRowCount());

                        int size = bucket.matchedSet.cardinality();
                        if (size <= 0)
                        {
                            continue;
                        }

                        TupleVector vectorToAppend = createNonMatchedOuterVector(bucket.vector);

                        if (nonMatchedBuilder == null)
                        {
                            nonMatchedBuilder = context.getVectorFactory()
                                    .getTupleVectorBuilder(size);
                        }
                        nonMatchedBuilder.append(vectorToAppend, bucket.matchedSet);
                    }
                }

                table = null;
                if (nonMatchedBuilder != null)
                {
                    return nonMatchedBuilder.build();
                }

                return null;
            }

            /** Build non matching outer vector. Used in inner hash mode */
            private TupleVector buildNonMatchedOuterVectorInnerHashMode(TupleVector vector, BitSet outerMatchedFilter, int probeRowCount)
            {
                Schema schema = getSchema(outerSchema, innerSchema);
                int outerSize = vector.getSchema()
                        .getSize();

                TupleVector vectorToAppend = new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return vector.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        if (column < outerSize)
                        {
                            return new ValueVectorAdapter(vector.getColumn(column))
                            {
                                @Override
                                public int size()
                                {
                                    return vector.getRowCount();
                                }
                            };
                        }
                        ResolvedType type = schema.getColumns()
                                .get(column)
                                .getType();
                        return ValueVector.literalNull(type, vector.getRowCount());
                    }
                };

                // Flip the filter to get non matches
                if (outerMatchedFilter != null)
                {
                    outerMatchedFilter.flip(0, probeRowCount);
                }

                int size = outerMatchedFilter == null ? vector.getRowCount()
                        : outerMatchedFilter.cardinality();

                ITupleVectorBuilder nonMatchedBuilder = context.getVectorFactory()
                        .getTupleVectorBuilder(size);
                // No matches for probed vector => return whole vector
                if (outerMatchedFilter == null)
                {
                    nonMatchedBuilder.append(vectorToAppend);
                }
                // Else build a new vector by fliping the filter to only pick those with no matches
                else
                {
                    nonMatchedBuilder.append(vectorToAppend, outerMatchedFilter);
                }
                return nonMatchedBuilder.build();
            }

            private TupleVector buildNonMatchedOuterVectorEmptyTable(TupleVector vector)
            {
                Schema schema = getSchema(vector.getSchema(), innerSchema);
                int outerSize = vector.getSchema()
                        .getSize();
                return new TupleVector()
                {
                    @Override
                    public Schema getSchema()
                    {
                        return schema;
                    }

                    @Override
                    public int getRowCount()
                    {
                        return vector.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        if (column < outerSize)
                        {
                            return vector.getColumn(column);
                        }
                        ResolvedType type = schema.getColumns()
                                .get(column)
                                .getType();
                        return ValueVector.literalNull(type, vector.getRowCount());
                    }
                };
            }
        };
    }

    // CSOFF
    private Schema getSchema(Schema outerSchema, Schema innerSchema)
    // CSON
    {
        if (!isAsteriskSchema)
        {
            return schema;
        }

        // If the inner schema is asterisk then just return the outer schema
        if (populateAlias == null
                && SchemaUtils.isAsterisk(innerSchema))
        {
            return outerSchema;
        }

        return SchemaUtils.joinSchema(outerSchema, innerSchema, populateAlias);
    }

    /**
     * Tuple vector that adapts the inner vectors ordinals with the outer schema. When running hash function of inner then ordinals are a bit off according to the inner input alone
     */
    private static class InnerTupleVector implements TupleVector
    {
        private final Schema schema;
        private final int outerSize;
        private final TupleVector inner;

        InnerTupleVector(Schema schema, int outerSize, TupleVector inner)
        {
            this.schema = schema;
            this.outerSize = outerSize;
            this.inner = inner;
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public int getRowCount()
        {
            return inner.getRowCount();
        }

        @Override
        public ValueVector getColumn(int column)
        {
            /*
             * @formatter:off
             * 
             * Join schema:    s.col1, s.col2, d.id, d.row_id, d.data
             * Ordinal:        0,      1,      2,    3,        4
             * Inner ordinal:                  0     1         2
             * 
             * @formatter:on
             */

            return inner.getColumn(column - outerSize);
        }
    }

    private static class ReplicatedTupleVector implements TupleVector
    {
        private final boolean outerIsHash;

        private Schema outerSchema;
        private Schema innerSchema;
        private TupleVector outer;
        private TupleVector inner;
        private HashValueBucket hashValueBucket;
        private int probeRow;
        private Schema schema;
        private int outerSize;
        private int rowCount;

        private ProbeAdapter[] columns;

        ReplicatedTupleVector(boolean outerIsHash)
        {
            this.outerIsHash = outerIsHash;
        }

        void init(TupleVector outer, TupleVector inner, HashValueBucket hashValueBucket, int probeRow, boolean isAsteriskSchema, Schema cartesianSchema)
        {
            this.outer = outer;
            this.inner = inner;
            this.hashValueBucket = hashValueBucket;
            this.probeRow = probeRow;
            this.rowCount = hashValueBucket.getRowCount();

            if (isAsteriskSchema)
            {
                boolean recalcualte = columns == null
                        || !(Objects.equals(outerSchema, outer.getSchema())
                                && Objects.equals(innerSchema, inner.getSchema()));

                // // Only re-calculate new schemas etc. if we switch schemas
                if (recalcualte)
                {
                    this.outerSchema = outer.getSchema();
                    this.innerSchema = inner.getSchema();
                    this.schema = SchemaUtils.joinSchema(outerSchema, innerSchema);
                }
            }
            else
            {
                this.schema = cartesianSchema;
                this.outerSchema = outer.getSchema();
                this.innerSchema = inner.getSchema();
            }
            this.outerSize = outerSchema.getSize();

            int columnsSize = this.outerSize + innerSchema.getSize();
            // Initalize new vectors
            if (columns == null)
            {
                columns = new ProbeAdapter[columnsSize];
            }
            else if (columns.length < columnsSize)
            {
                columns = Arrays.copyOf(columns, columnsSize);
            }
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public int getRowCount()
        {
            return rowCount;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            final boolean isOuter = column < outerSize;
            ValueVector v;
            if (isOuter)
            {
                v = outer.getColumn(column);
            }
            else
            {
                v = inner.getColumn(column - outerSize);
            }

            ProbeAdapter adapter = columns[column];
            if (adapter == null)
            {
                adapter = new ProbeAdapter(v);
                columns[column] = adapter;
            }

            adapter.setVector(v);
            adapter.probeRow = probeRow;
            adapter.rowCount = rowCount;
            adapter.isOuter = isOuter;
            adapter.outerIsHash = outerIsHash;
            adapter.hashValueBucket = hashValueBucket;

            return adapter;
        }

        static class ProbeAdapter extends ValueVectorAdapter
        {
            int probeRow;
            int rowCount;
            boolean isOuter;
            boolean outerIsHash;
            HashValueBucket hashValueBucket;

            ProbeAdapter(ValueVector v)
            {
                super(v);
            }

            @Override
            public int size()
            {
                return rowCount;
            }

            @Override
            protected int getRow(int row)
            {
                if (isOuter)
                {
                    return outerIsHash ? hashValueBucket.getInt(row)
                            : probeRow;
                }

                return outerIsHash ? probeRow
                        : hashValueBucket.getInt(row);
            }
        }
    }

    /** Node data for hash match */
    private static class HashNodeData extends NodeData
    {
        // Nanos
        long hashTime;
        long probeTime;
    }

    /** Bucket in {@link HashValue} with row indices along with owning vector */
    private static class HashValueBucket implements TupleVector
    {
        /** Unique counter for vector */
        private final int vectorId;
        private final TupleVector vector;
        /** A bit set for all matched rows in this buckets vector. There can be many buckets with the same vector */
        private final BitSet matchedSet;
        private IntList rowIndices;

        /** List with bitset per {@link #rowIndices} with matched inner rows */
        private List<BitSet> populatedMatches;

        HashValueBucket(int vectorId, TupleVector vector, BitSet matchedSet)
        {
            this.vectorId = vectorId;
            this.vector = vector;
            this.matchedSet = matchedSet;
        }

        @Override
        public Schema getSchema()
        {
            return vector.getSchema();
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return new ValueVectorAdapter(vector.getColumn(column))
            {
                @Override
                public int size()
                {
                    return rowIndices.size();
                }

                @Override
                protected int getRow(int row)
                {
                    return rowIndices.getInt(row);
                }
            };
        }

        @Override
        public int getRowCount()
        {
            return rowIndices.size();
        }

        int getInt(int row)
        {
            return rowIndices.getInt(row);
        }

        void add(int rowIndex)
        {
            if (rowIndices == null)
            {
                rowIndices = IntList.of(rowIndex);
                return;
            }
            else if (rowIndices.size() == 1)
            {
                IntList tmp = new IntArrayList();
                tmp.add(rowIndices.getInt(0));
                rowIndices = tmp;
            }
            rowIndices.add(rowIndex);
        }

        /** Marks matched rows. Used in outer hash mode when emitting empty outer rows */
        void markMatch(ValueVector filter)
        {
            int size = filter.size();
            for (int i = 0; i < size; i++)
            {
                if (filter.getPredicateBoolean(i))
                {
                    matchedSet.set(rowIndices.getInt(i));
                }
            }
        }

        /** Marks populated matches in outer hash mode */
        void markPopulatedMatchesOuterHash(ValueVector filter, int index)
        {
            if (populatedMatches == null)
            {
                populatedMatches = new ArrayList<>(Collections.nCopies(getRowCount(), null));
            }
            int filterSize = filter.size();
            for (int i = 0; i < filterSize; i++)
            {
                if (!filter.getPredicateBoolean(i))
                {
                    continue;
                }

                BitSet bitSet = populatedMatches.get(i);
                if (bitSet == null)
                {
                    bitSet = new BitSet();
                    populatedMatches.set(i, bitSet);
                }

                bitSet.set(index);
            }
        }

        /** Marks populated matches in inner hash mode */
        static void markPopulatedMatchesInnerHash(HashValueBucket bucket, List<BitSet> populatedMatches, ValueVector filter, int index)
        {
            BitSet bitSet = populatedMatches.get(index);
            if (bitSet == null)
            {
                bitSet = new BitSet();
                populatedMatches.set(index, bitSet);
            }

            int filterSize = filter.size();
            for (int i = 0; i < filterSize; i++)
            {
                if (!filter.getPredicateBoolean(i))
                {
                    continue;
                }

                bitSet.set(bucket.rowIndices.getInt(i));
            }
        }
    }

    /** Value in the hash table */
    private static class HashValue
    {
        /** This values buckets with row indices */
        private List<HashValueBucket> buckets;
        private HashValueBucket topBucket;

        void add(int vectorId, TupleVector vector, BitSet matchedSet, int rowIndex)
        {
            if (topBucket == null)
            {
                topBucket = new HashValueBucket(vectorId, vector, matchedSet);
                buckets = singletonList(topBucket);
            }
            // We switched vector, then add a new vector to list
            else if (topBucket.vectorId != vectorId)
            {
                // Switch from singleton list to array list
                if (buckets.size() == 1)
                {
                    List<HashValueBucket> tmp = new ArrayList<>();
                    tmp.add(topBucket);
                    buckets = tmp;
                }
                topBucket = new HashValueBucket(vectorId, vector, matchedSet);

                buckets.add(topBucket);
            }

            topBucket.add(rowIndex);
        }
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return List.of(outer, inner);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return asList(outer, inner);
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof HashMatch that)
        {
            return nodeId == that.nodeId
                    && outer.equals(that.outer)
                    && inner.equals(that.inner)
                    && outerHashFunction.equals(that.outerHashFunction)
                    && innerHashFunction.equals(that.innerHashFunction)
                    && condition.equals(that.condition)
                    && Objects.equals(populateAlias, that.populateAlias)
                    && emitEmptyOuterRows == that.emitEmptyOuterRows
                    && pushOuterReference == that.pushOuterReference;

        }
        return false;
    }

    @Override
    public String toString()
    {
        String type = emitEmptyOuterRows ? "LEFT"
                : "INNER";

        return "Hash Match " + "("
               + nodeId
               + "): "
               + ", logical: "
               + type
               + (condition != null ? ", condition: " + condition.toString()
                       : "")
               + (populateAlias != null ? ", populate (" + populateAlias + ")"
                       : "")
               + (pushOuterReference ? ", pushOuterReference"
                       : "");
    }
}
