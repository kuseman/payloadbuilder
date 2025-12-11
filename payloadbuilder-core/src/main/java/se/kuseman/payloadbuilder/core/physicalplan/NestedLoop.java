package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ObjectUtils.getIfNull;

import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/**
 * Nested loop implementation of a physical plan
 * 
 * <pre>
 * Logical operations this operator supports:
 * 
 * - INNER JOIN
 *    - No outer references
 *    - Condition
 *    - Populate
 *    - Don't emit empty outer rows
 * - LEFT JOIN
 *    - No outer references
 *    - Condition
 *    - Populate
 *    - Emit empty outer rows
 * - CROSS JOIN
 *    - No outer references
 *    - No condition
 *    - Populate
 *    - Don't emit empty outer rows
 *    
 *  -- Row by row operations.
 *  -- Loops each outer row and executes the inner plan for each
 *    
 * - OUTER APPLY
 *    - Can have outer references
 *    - No condition
 *    - Populate
 *    - Emit empty outer rows
 * - CROSS APPLY
 *    - Can have outer references
 *    - No condition
 *    - Populate
 *    - Don't emit empty outer rows
 * </pre>
 */
public class NestedLoop implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan outer;
    private final IPhysicalPlan inner;
    /** Join predicate */
    private final BiFunction<TupleVector, IExecutionContext, ValueVector> condition;
    /** The alias if this is a populating join */
    private final String populateAlias;
    /** Set with the outer references (if any) that the inner side references. If set we will go the outer reference path when looping */
    private final Set<Column> outerReferences;
    /** Utilizes outer reference. Pushes outer reference into context before executing inner. Is done per row in outer. This mode has no join predicate */
    private final boolean pushOuterReference;
    /** Should empty outer rows be emited. True when having left join/outer apply. */
    private final boolean emitEmptyOuterRows;
    /** Flag that indicates that inner and outer has switched places so we need to take that into consideration when generating schema and concating vectors */
    private final boolean switchedInputs;
    /** The outer schema if this join is a correlated type */
    private final Schema outerSchema;

    private final Schema schema;
    private final Schema cartesianSchema;
    private final boolean isAsteriskSchema;

    private final boolean outerSchemaOriginatesFromAsteriskInput;
    private final boolean schemaOriginatesFromAsteriskInput;

    //@formatter:off
    private NestedLoop(
            int nodeId,
            IPhysicalPlan outer,
            IPhysicalPlan inner,
            BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            String populateAlias,
            Set<Column> outerReferences,
            boolean emitEmptyOuterRows,
            boolean switchedInputs,
            boolean pushOuterReference,
            Schema outerSchema)
    //@formatter:on
    {
        this.nodeId = nodeId;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "innter");
        this.condition = condition;
        this.populateAlias = populateAlias;
        this.outerReferences = getIfNull(outerReferences, emptySet());
        this.pushOuterReference = pushOuterReference
                || this.outerReferences.size() > 0;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.switchedInputs = switchedInputs;
        this.outerSchema = requireNonNull(outerSchema, "outerSchema");
        this.outerSchemaOriginatesFromAsteriskInput = SchemaUtils.originatesFromAsteriskInput(outerSchema);

        if (switchedInputs
                && (condition != null
                        || populateAlias != null))
        {
            throw new IllegalArgumentException("Switched inputs are only supported for non conditional loops.");
        }
        else if (pushOuterReference
                && (outerReferences == null
                        || outerReferences.size() == 0)
                && condition == null)
        {
            throw new UnsupportedOperationException("Push outer reference needs a condition");
        }

        this.schema = getSchema();
        this.cartesianSchema = getSchema(true);
        this.isAsteriskSchema = SchemaUtils.isAsterisk(schema);
        this.schemaOriginatesFromAsteriskInput = SchemaUtils.originatesFromAsteriskInput(schema);
    }

    /** Create an inner join. Logical operations: INNER JOIN */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition, String populateAlias,
            boolean pushOuterReference)
    {
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, null, false, false, pushOuterReference, Schema.EMPTY);
    }

    /** Create an inner join. Logical operations: INNER JOIN with outer references. This is used when having an expression scan as inner. */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            String populateAlias, boolean pushOuterReference)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, outerReferences, false, false, pushOuterReference, Schema.EMPTY);
    }

    /** Create an inner join with no condition and no outer references. Logical operations: CROSS JOIN/CROSS APPLY */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, String populateAlias, boolean switchedInputs)
    {
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, null, false, switchedInputs, false, Schema.EMPTY);
    }

    /** Create an inner join with no condition with outer references. Logical operations: CROSS JOIN/CROSS APPLY */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, String populateAlias, Schema outerSchema)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, outerReferences, false, false, true, outerSchema);
    }

    /** Create a left outer join. Logical operations: LEFT OUTER JOIN with outer references. This is used when having an expression scan as inner. */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, BiFunction<TupleVector, IExecutionContext, ValueVector> condition,
            String populateAlias, boolean pushOuterReference)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, outerReferences, true, false, pushOuterReference, Schema.EMPTY);
    }

    /** Create a left outer join. Logical operations: LEFT OUTER JOIN */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition, String populateAlias,
            boolean pushOuterReference)
    {
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, null, true, false, pushOuterReference, Schema.EMPTY);
    }

    /** Create a left outer join with no condition and no outer references. Logical operations: OUTER APPLY */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, String populateAlias, boolean switchedInputs)
    {
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, null, true, switchedInputs, false, Schema.EMPTY);
    }

    /** Create an left outer join with no condition with outer references. Logical operations: OUTER APPLY */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, String populateAlias, Schema outerSchema)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, outerReferences, true, false, true, outerSchema);
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Nested Loop";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema()));
        LoopNodeData nodeData = context.getStatementContext()
                .getNodeData(nodeId);
        if (nodeData != null)
        {
            properties.put("Predicate Time", DurationFormatUtils.formatDurationHMS(nodeData.predicateTime));
            properties.put("Tuple Build Time", DurationFormatUtils.formatDurationHMS(nodeData.tupleBuildTime));
        }

        properties.put(IDatasource.PREDICATE, condition != null ? condition.toString()
                : null);
        properties.put("Switched Inputs", switchedInputs);
        properties.put("Outer References", outerReferences);
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
        Schema outerSchema = switchedInputs ? inner.getSchema()
                : outer.getSchema();

        Schema innerSchema = switchedInputs ? outer.getSchema()
                : inner.getSchema();

        return SchemaUtils.joinSchema(outerSchema, innerSchema, cartesian ? null
                : populateAlias);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // Extract outer tuple vector before executing outer in case it's cleared inside that branch
        TupleVector outerTupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();

        final TupleIterator outerIt = outer.execute(context);

        if (!outerIt.hasNext())
        {
            outerIt.close();
            return TupleIterator.EMPTY;
        }

        final LoopNodeData nodeData = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new LoopNodeData());

        if (!outerReferences.isEmpty())
        {
            return new LoopTupleIterator((ExecutionContext) context, outerIt, outerTupleVector, nodeData);
        }
        else if (populateAlias != null)
        {
            return new PopulatingTupleIterator((ExecutionContext) context, outerIt, nodeData);
        }

        /* @formatter:off
         * Create a regular iterator. Simply fetches outer and inner batches in a loop fashion and evaluates
         *
         * Usage (non populating): 
         *   - CROSS JOIN 
         *   - INNER JOIN 
         *   - LEFT JOIN
         *
         * @formatter:on
         */

        final ExecutionContext executionContext = (ExecutionContext) context;

        return new TupleIterator()
        {
            private TupleIterator innerIt;
            private TupleVector currentOuter;
            private TupleVector currentInner;
            private TupleVector next;
            /** Bit set to keep track of outer matches. Used in left joins to know what outer indices to return */
            private BitSet outerMatches;
            private CartesianTupleVector cartesian = new CartesianTupleVector(cartesianSchema, schemaOriginatesFromAsteriskInput);

            // The inner schema used when emitting empty outer row, will be the plan schema from start
            // but if there are inner matches before the un matched ones we switch
            private Schema innerSchema = switchedInputs ? outer.getSchema()
                    : inner.getSchema();
            private boolean innerSchemaAsterisk = SchemaUtils.isAsterisk(innerSchema);

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector result = next;
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
                if (innerIt != null)
                {
                    innerIt.close();
                }
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (context.getSession()
                            .abortQuery())
                    {
                        return false;
                    }

                    if (innerIt == null
                            && !outerIt.hasNext())
                    {
                        // Done
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = outerIt.next();

                        if (currentOuter.getRowCount() <= 0)
                        {
                            currentOuter = null;
                            continue;
                        }

                        if (emitEmptyOuterRows)
                        {
                            outerMatches = new BitSet(currentOuter.getRowCount());
                        }
                    }
                    if (innerIt == null)
                    {
                        ExecutionContext ctx = executionContext;
                        if (pushOuterReference)
                        {
                            // Make a copy of the execution context here because we mutable the outer tuple vector
                            // and we risk evaluating against wrong tuple otherwise if expressions are lazy etc.
                            ctx = ctx.copy();
                            ctx.getStatementContext()
                                    .setIndexSeekTupleVector(currentOuter);
                        }

                        innerIt = inner.execute(ctx);
                        continue;
                    }
                    else if (!innerIt.hasNext())
                    {
                        next = emitEmptyOuterRows ? createUnmatchedOuterTuple(executionContext, innerSchema, outerMatches, currentOuter)
                                : null;
                        innerIt.close();
                        innerIt = null;
                        currentOuter = null;
                        continue;
                    }
                    currentInner = innerIt.next();

                    if (currentInner.getRowCount() <= 0)
                    {
                        currentInner = null;
                        continue;
                    }

                    if (innerSchemaAsterisk
                            && currentInner.getRowCount() > 0)
                    {
                        // Switch the inner schema
                        innerSchema = currentInner.getSchema();
                    }

                    if (condition == null)
                    {
                        // Switched inputs means the planner/optimizer has switched places on the inputs and this needs to
                        // be corrected when we return tuples to keep the schema consistent.
                        // This happens for example when we have non correlated sub query expression that is placed in the
                        // outer plan (because it's non correlated and only needs to be executed once) when it logically belongs to the inner.
                        // Also right joins that is transformed into left joins then we switch inputs
                        if (switchedInputs)
                        {
                            next = VectorUtils.cartesian(currentInner, currentOuter);
                        }
                        else
                        {
                            next = VectorUtils.cartesian(currentOuter, currentInner);
                        }

                        // Mark all outer rows as matched since we have no predicate
                        if (emitEmptyOuterRows)
                        {
                            int outerRowCount = currentOuter.getRowCount();
                            for (int i = 0; i < outerRowCount; i++)
                            {
                                outerMatches.set(i);
                            }
                        }
                    }
                    else
                    {
                        long time = System.nanoTime();
                        // First construct a cartesian tuple vector that will be the one we run the predicate against
                        cartesian.init(currentOuter, currentInner);
                        ValueVector filter = condition.apply(cartesian, context);

                        int cardinality = filter.getCardinality();

                        nodeData.predicateTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        if (cardinality > 0)
                        {
                            time = System.nanoTime();
                            // Build a matching vector from cartesian and filter
                            ITupleVectorBuilder b = context.getVectorFactory()
                                    .getTupleVectorBuilder(cardinality);
                            b.append(cartesian, filter);

                            // Mark matched outer rows
                            if (emitEmptyOuterRows)
                            {
                                markMatchedRows(outerMatches, filter, currentInner);

                                // If this was the last inner vector, create an unmatched tuple and include in current builder to avoid another loop
                                if (!innerIt.hasNext())
                                {
                                    TupleVector unmatchedOuter = createUnmatchedOuterTuple(executionContext, innerSchema, outerMatches, currentOuter);
                                    if (unmatchedOuter != null)
                                    {
                                        b.append(unmatchedOuter);
                                    }
                                    innerIt.close();
                                    innerIt = null;
                                    currentOuter = null;
                                }
                            }

                            next = b.build();
                            nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        }
                    }
                }
                return true;
            }
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return asList(outer, inner);
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
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof NestedLoop that)
        {
            return nodeId == that.nodeId
                    && inner.equals(that.inner)
                    && outer.equals(that.outer)
                    && Objects.equals(condition, that.condition)
                    && Objects.equals(populateAlias, that.populateAlias)
                    && outerReferences.equals(that.outerReferences)
                    && pushOuterReference == that.pushOuterReference
                    && emitEmptyOuterRows == that.emitEmptyOuterRows
                    && switchedInputs == that.switchedInputs
                    && outerSchema.equals(that.outerSchema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        String type = emitEmptyOuterRows ? "LEFT"
                : "INNER";

        return "Nested loop " + "("
               + nodeId
               + "): "
               + ", logical: "
               + type
               + (condition != null ? ", condition: " + condition.toString()
                       : "")
               + (populateAlias != null ? ", populate (" + populateAlias + ")"
                       : "")
               + (!outerReferences.isEmpty() ? ", outerReferences: " + outerReferences
                       : "")
               + (switchedInputs ? " switched inputs"
                       : "")
               + (pushOuterReference ? ", pushOuterReference"
                       : "")
               + (outerSchema.getSize() > 0 ? ", outer schema: " + outerSchema
                       : "");
    }

    /**
     * Create an populating iterator. This one needs all inner tuples before each outer can be streamed
     *
     * <pre>
     * When doing a populated nested loop join we need all inner tuples for each outer before we can return
     * a resulting TupleVector. This requires that we store all inner tuples in memory.
     * 
     * Usage:
     *  - INNER JOIN
     *  - LEFT JOIN
     *  - CROSS JOIN
     * 
     * </pre>
     */
    private class PopulatingTupleIterator implements TupleIterator
    {
        private final ExecutionContext context;
        private final TupleIterator iterator;
        private final LoopNodeData nodeData;

        private TupleVector concatOfInner;
        private TupleVector currentOuter;
        private TupleVector next;
        /** Bit set to keep track of outer matches. Used in left joins to know what outer indices to return */
        private BitSet outerMatches;
        private CartesianTupleVector cartesian = new CartesianTupleVector(cartesianSchema, schemaOriginatesFromAsteriskInput);

        // The inner schema used when emitting empty outer row, will be the plan schema from start
        // but if there are inner matches before the un matched ones we switch
        private Schema innerSchema = inner.getSchema();
        private boolean innerSchemaAsterisk = SchemaUtils.isAsterisk(innerSchema);

        PopulatingTupleIterator(ExecutionContext context, TupleIterator iterator, LoopNodeData nodeData)
        {
            this.context = context;
            this.iterator = iterator;
            this.nodeData = nodeData;
        }

        @Override
        public TupleVector next()
        {
            if (next == null)
            {
                throw new NoSuchElementException();
            }
            TupleVector result = next;
            next = null;
            return result;
        }

        @Override
        public boolean hasNext()
        {
            // Concat all inner tuples once if we don't need to push outer reference
            if (concatOfInner == null
                    && !pushOuterReference)
            {
                concatOfInner = PlanUtils.concat(context, inner.execute(context));
                int rowCount = concatOfInner.getRowCount();
                if (rowCount == 0
                        && !emitEmptyOuterRows)
                {
                    return false;
                }
                else if (innerSchemaAsterisk
                        && rowCount > 0)
                {
                    // We have an inner schema, switch the plan schema to the runtime one
                    innerSchema = concatOfInner.getSchema();
                }
            }

            return setNext();
        }

        @Override
        public void close()
        {
            iterator.close();
        }

        private boolean setNext()
        {
            while (next == null)
            {
                if (context.getSession()
                        .abortQuery())
                {
                    return false;
                }

                if (!iterator.hasNext())
                {
                    return false;
                }
                else if (currentOuter == null)
                {
                    currentOuter = iterator.next();

                    if (currentOuter.getRowCount() <= 0)
                    {
                        currentOuter = null;
                        continue;
                    }

                    if (emitEmptyOuterRows)
                    {
                        outerMatches = new BitSet(currentOuter.getRowCount());
                    }
                    if (currentOuter.getRowCount() == 0
                            || (concatOfInner != null
                                    && concatOfInner.getRowCount() == 0))
                    {
                        next = emitEmptyOuterRows ? createUnmatchedOuterTuple(context, innerSchema, outerMatches, currentOuter)
                                : null;
                        currentOuter = null;
                        continue;
                    }
                }

                if (condition == null)
                {
                    // NOTE! No need to mark any unmatched outer rows here since when condition is null emitEmptyOuterRows is never true
                    // NOTE! Push outer reference is never true here
                    next = VectorUtils.populateCartesian(currentOuter, concatOfInner, populateAlias);
                }
                else
                {
                    TupleVector concatOfInner = this.concatOfInner;
                    if (pushOuterReference)
                    {
                        ExecutionContext ctx = context;
                        // Make a copy of the execution context here because we mutable the outer tuple vector
                        // and we risk evaluating against wrong tuple otherwise if expressions are lazy etc.
                        ctx = ctx.copy();
                        ctx.getStatementContext()
                                .setIndexSeekTupleVector(currentOuter);

                        concatOfInner = PlanUtils.concat(context, inner.execute(ctx));

                        int rowCount = concatOfInner.getRowCount();
                        if (rowCount == 0)
                        {
                            next = emitEmptyOuterRows ? createUnmatchedOuterTuple(context, innerSchema, outerMatches, currentOuter)
                                    : null;
                            currentOuter = null;
                            continue;
                        }
                        else if (innerSchemaAsterisk
                                && rowCount > 0)
                        {
                            // We have an inner schema, switch the plan schema to the runtime one
                            innerSchema = concatOfInner.getSchema();
                        }
                    }

                    long time = System.nanoTime();
                    // First construct a cartesian tuple vector that will be the one we run the predicate against
                    cartesian.init(currentOuter, concatOfInner);
                    ValueVector filter = condition.apply(cartesian, context);

                    nodeData.predicateTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);

                    time = System.nanoTime();

                    if (emitEmptyOuterRows)
                    {
                        markMatchedRows(outerMatches, filter, concatOfInner);
                    }

                    TupleVector unmatched = emitEmptyOuterRows ? createUnmatchedOuterTuple(context, innerSchema, outerMatches, currentOuter)
                            : null;

                    if (filter.getCardinality() == 0)
                    {
                        next = unmatched;
                    }
                    else
                    {
                        ITupleVectorBuilder b = context.getVectorFactory()
                                .getTupleVectorBuilder(currentOuter.getRowCount());
                        b.appendPopulate(currentOuter, concatOfInner, filter, populateAlias);
                        if (unmatched != null)
                        {
                            b.append(unmatched);
                        }

                        next = b.build();
                    }

                    nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                }
                currentOuter = null;
            }

            return true;
        }
    }
    // }

    /**
     * Create an loop iterator where inner is executed row by row and not vector by vector.
     *
     * <pre>
     * Usage:
     *  - Correlated sub queries in projections
     *  - CROSS APPLY
     *  - OUTER APPLY
     * This mode has no predicate. The outer reference is pushed into inner all returned rows
     * are the matched ones
     * </pre>
     */

    private class LoopTupleIterator implements TupleIterator
    {
        private final ExecutionContext context;
        private final TupleIterator iterator;
        private final LoopNodeData nodeData;

        /**
         * <pre>
         * We use mutable vectors here during joins this to lower the amount of allocations.
         * This is a risk but there are safe guards
         *  - We seal the vectors when we return resulting vectors which means that wrongly implemented
         *    functions/expressions will crash and we catch this early
         *  - In each iteration we copy the result vectors values and hence we are safe to modify the state
         *    between iterations
         * </pre>
         */
        private final OuterTupleVector outerTupleVector;
        private final RowTupleVector rowTupleVector = new RowTupleVector();
        private final CartesianTupleVector cartesian = new CartesianTupleVector(cartesianSchema, schemaOriginatesFromAsteriskInput);

        private TupleVector currentOuter;
        private TupleVector next;
        private TupleVector nextUnmatchedOuter;

        // The inner schema used when emitting empty outer row, will be the plan schema from start
        // but if there are inner matches before the un matched ones we switch
        private Schema innerSchema = inner.getSchema();
        private boolean innerSchemaAsterisk = SchemaUtils.isAsterisk(innerSchema);

        LoopTupleIterator(ExecutionContext context, TupleIterator iterator, TupleVector contextOuterTupleVector, LoopNodeData nodeData)
        {
            this.context = context;
            this.iterator = iterator;
            this.nodeData = nodeData;
            this.outerTupleVector = new OuterTupleVector(contextOuterTupleVector, outerSchema, outerSchemaOriginatesFromAsteriskInput);
        }

        @Override
        public TupleVector next()
        {
            if (next == null)
            {
                throw new NoSuchElementException();
            }
            // We seal the mutable vectors when we return the result, this to catch
            // wrongly implemented function/expressions
            rowTupleVector.sealed = true;
            TupleVector result = next;
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
            rowTupleVector.sealed = true;
            iterator.close();
        }

        private boolean setNext()
        {
            while (next == null)
            {
                if (context.getSession()
                        .abortQuery())
                {
                    return false;
                }

                if (nextUnmatchedOuter != null)
                {
                    next = nextUnmatchedOuter;
                    nextUnmatchedOuter = null;
                    continue;
                }

                if (!iterator.hasNext())
                {
                    return false;
                }
                else if (currentOuter == null)
                {
                    currentOuter = iterator.next();
                }

                int rowCount = currentOuter.getRowCount();

                if (rowCount <= 0)
                {
                    currentOuter = null;
                    continue;
                }

                BitSet outerMatches = null;
                if (emitEmptyOuterRows)
                {
                    outerMatches = new BitSet(rowCount);
                }
                // CSOFF
                ITupleVectorBuilder builder = null;
                // CSON

                rowTupleVector.sealed = false;
                rowTupleVector.init(currentOuter);
                outerTupleVector.init(rowTupleVector);

                for (int i = 0; i < rowCount; i++)
                {
                    if (context.getSession()
                            .abortQuery())
                    {
                        return false;
                    }

                    rowTupleVector.row = i;

                    // Make a copy of the execution context here because we mutate the outer tuple vector
                    // and we risk evaluating against wrong outer tuple otherwise
                    ExecutionContext executionContext = context;
                    if (pushOuterReference)
                    {
                        executionContext = executionContext.copy();
                        executionContext.getStatementContext()
                                .setOuterTupleVector(outerTupleVector);
                    }

                    TupleIterator innerIt = inner.execute(executionContext);
                    boolean matched = false;

                    if (populateAlias != null)
                    {
                        TupleVector concatOfInner = PlanUtils.concat(executionContext, innerIt);
                        if (concatOfInner.getRowCount() > 0)
                        {
                            // Use the first vectors schema as inner
                            if (innerSchemaAsterisk)
                            {
                                innerSchema = concatOfInner.getSchema();
                                innerSchemaAsterisk = false;
                            }

                            // CSOFF
                            long time = System.nanoTime();
                            // CSON

                            if (builder == null)
                            {
                                builder = context.getVectorFactory()
                                        .getTupleVectorBuilder(rowCount);
                            }

                            cartesian.init(rowTupleVector, concatOfInner);

                            // All true filter
                            ValueVector filter = ValueVector.literalBoolean(true, cartesian.rowCount);
                            matched = true;
                            // .. or a condition
                            // Conditions in a loop iterator is when we have an expression scan along with
                            // inner/left join, then we are both iterating outer plus evaluating
                            if (condition != null)
                            {
                                filter = condition.apply(cartesian, context);
                                matched = filter.getCardinality() > 0;
                            }

                            builder.appendPopulate(rowTupleVector, concatOfInner, filter, populateAlias);
                            nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        }
                    }
                    else
                    {
                        try
                        {
                            while (innerIt.hasNext())
                            {
                                TupleVector inner = innerIt.next();
                                int innerRowCount = inner.getRowCount();
                                if (innerRowCount > 0)
                                {
                                    // Use the first vectors schema as inner
                                    if (innerSchemaAsterisk)
                                    {
                                        innerSchema = inner.getSchema();
                                        innerSchemaAsterisk = false;
                                    }

                                    // CSOFF
                                    long time = System.nanoTime();
                                    // CSON

                                    cartesian.init(rowTupleVector, inner);

                                    // All true filter
                                    ValueVector filter = ValueVector.literalBoolean(true, cartesian.rowCount);
                                    matched = true;
                                    // .. or a condition
                                    // Conditions in a loop iterator is when we have an expression scan along with
                                    // inner/left join, then we are both iterating outer plus evaluating
                                    if (condition != null)
                                    {
                                        filter = condition.apply(cartesian, context);
                                        matched = filter.getCardinality() > 0;
                                    }

                                    if (builder == null)
                                    {
                                        builder = context.getVectorFactory()
                                                .getTupleVectorBuilder((int) (rowCount * inner.getRowCount() * 1.1));
                                    }

                                    builder.append(cartesian, filter);
                                    nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                                }
                            }
                        }
                        finally
                        {
                            innerIt.close();
                        }
                    }

                    if (emitEmptyOuterRows)
                    {
                        outerMatches.set(i, matched);
                    }
                }

                // CSOFF
                long time = System.nanoTime();
                // CSON

                TupleVector unmatchedOuter = emitEmptyOuterRows ? createUnmatchedOuterTuple(context, innerSchema, outerMatches, currentOuter)
                        : null;

                if (next != null)
                {
                    nextUnmatchedOuter = unmatchedOuter;
                }
                else
                {
                    if (builder == null)
                    {
                        next = unmatchedOuter;
                    }
                    else
                    {
                        if (unmatchedOuter != null)
                        {
                            builder.append(unmatchedOuter);
                        }
                        next = builder.build();
                    }
                }
                currentOuter = null;
                nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
            }

            return true;
        }
    }

    /** Creates a tuple vector with non matched outer rows. Used i left joins */
    private TupleVector createUnmatchedOuterTuple(ExecutionContext context, Schema innerSchema, BitSet outerMatches, TupleVector outer)
    {
        // Flip all bits then all set bits are the unmatched outer rows
        int rowCount = outer.getRowCount();
        outerMatches.flip(0, rowCount);

        int cardinality = outerMatches.cardinality();
        // No unmatched outer rows
        if (cardinality == 0)
        {
            return null;
        }

        TupleVector unmatchedOuter;
        if (cardinality == outer.getRowCount())
        {
            unmatchedOuter = outer;
        }
        else
        {
            // Build an outer vector with unmatched rows
            ITupleVectorBuilder b = context.getVectorFactory()
                    .getTupleVectorBuilder(cardinality);
            b.append(outer, outerMatches);
            unmatchedOuter = b.build();
        }

        Schema s = this.schema;
        if (this.isAsteriskSchema)
        {
            // If the inner schema is asterisk then just return the unmatched outer vector
            // We do this if we have a non populated join because a populated join has an inner schema
            // even if we have an asterisk query
            if (populateAlias == null
                    && SchemaUtils.isAsterisk(innerSchema))
            {
                return unmatchedOuter;
            }

            s = SchemaUtils.joinSchema(outer.getSchema(), innerSchema, populateAlias);
        }
        final Schema schema = s;
        final int outerSize = outer.getSchema()
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
                return unmatchedOuter.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                // Return null vector
                if (column >= outerSize)
                {
                    ResolvedType type = schema.getColumns()
                            .get(column)
                            .getType();
                    return ValueVector.literalNull(type, unmatchedOuter.getRowCount());
                }
                return unmatchedOuter.getColumn(column);
            }
        };
    }

    private void markMatchedRows(BitSet outerMatches, ValueVector filter, TupleVector inner)
    {
        // Loop all outer chunks and if one if the bits is set we have an outer match
        int innerRowCount = inner.getRowCount();
        int filterSize = filter.size();
        int chunkSize = filterSize / innerRowCount;

        for (int i = 0; i < chunkSize; i++)
        {
            // Outer row already matched no need to check the filter
            if (outerMatches.get(i))
            {
                continue;
            }

            int chunkStart = i * innerRowCount;
            int chunkEnd = chunkStart + innerRowCount;

            for (int j = chunkStart; j < chunkEnd; j++)
            {
                // If we have one match in the current outer chunk we don't need to check any more
                if (filter.getPredicateBoolean(j))
                {
                    outerMatches.set(i);
                    break;
                }
            }
        }
    }

    /** Node data for nested loop */
    private static class LoopNodeData extends NodeData
    {
        long predicateTime;
        long tupleBuildTime;
    }

    /** Vector implementation used when having outer references chain in loop */
    private static class OuterTupleVector implements TupleVector
    {
        private final TupleVector prevOuter;
        private final Schema prevOuterSchema;
        private final int prevOuterSize;
        private final Schema planOuterSchema;
        private final boolean outerSchemaIsAsterisk;

        private TupleVector outer;
        private Schema outerSchema;
        private Schema schema;

        OuterTupleVector(TupleVector prevOuter, Schema planOuterSchema, boolean outerSchemaIsAsterisk)
        {
            this.prevOuter = prevOuter;
            this.prevOuterSchema = prevOuter != null ? prevOuter.getSchema()
                    : null;
            this.prevOuterSize = prevOuter != null ? prevOuter.getSchema()
                    .getSize()
                    : 0;

            this.planOuterSchema = planOuterSchema;
            this.outerSchemaIsAsterisk = outerSchemaIsAsterisk;
        }

        void init(TupleVector outer)
        {
            if (prevOuter != null
                    && outer.getRowCount() != prevOuter.getRowCount())
            {
                throw new IllegalArgumentException("Vectors must have equal row counts");
            }

            this.outer = outer;

            // Re-create the resulting schema if changed since last iteration
            if (!Objects.equals(outerSchema, outer.getSchema()))
            {
                this.outerSchema = outer.getSchema();
                this.schema = SchemaUtils.joinSchema(prevOuterSchema, outerSchema);
            }

            // We only assert that they are equal
            if (!outerSchemaIsAsterisk)
            {
                CartesianTupleVector.assertSchemas(planOuterSchema, schema, "outer");
            }
        }

        @Override
        public int getRowCount()
        {
            return outer.getRowCount();
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            if (column < prevOuterSize)
            {
                return prevOuter.getColumn(column);
            }

            return outer.getColumn(column - prevOuterSize);
        }
    }

    /**
     * Implementation of TupleVector that makes a cartesian product between to vectors. This by not copying the contents but instead replicates the columns.
     */
    private static class CartesianTupleVector implements TupleVector
    {
        private final Schema cartesianSchema;
        private final boolean schemaOriginatesFromAsteriskInput;

        private TupleVector outer;
        private TupleVector inner;
        private Schema outerSchema;
        private Schema innerSchema;

        /** Resulting schema of the joined vectors */
        private Schema schema;
        private CartesianColumn[] columns;
        private int rowCount;
        private int outerSize;
        private int innerRowCount;

        CartesianTupleVector(Schema cartesianSchema, boolean schemaOriginatesFromAsteriskInput)
        {
            this.cartesianSchema = cartesianSchema;
            this.schemaOriginatesFromAsteriskInput = schemaOriginatesFromAsteriskInput;
        }

        /** Inits this vector with new outer/inner vectors. Calculates new schemas etc. */
        void init(TupleVector outer, TupleVector inner)
        {
            this.outer = outer;
            this.inner = inner;
            this.rowCount = outer.getRowCount() * inner.getRowCount();
            this.innerRowCount = inner.getRowCount();

            // Recalculate resulting schema if differs
            if (!Objects.equals(outerSchema, outer.getSchema())
                    || !Objects.equals(innerSchema, inner.getSchema()))
            {
                this.outerSchema = outer.getSchema();
                this.innerSchema = inner.getSchema();
                this.schema = SchemaUtils.joinSchema(outerSchema, innerSchema);
            }

            // We only assert that they are equal
            if (!schemaOriginatesFromAsteriskInput)
            {
                assertSchemas(cartesianSchema, schema, "cartesian");
            }
            this.outerSize = outerSchema.getSize();

            int size = schema.getSize();
            if (columns == null)
            {
                columns = new CartesianColumn[size];
            }
            else if (columns.length < size)
            {
                columns = Arrays.copyOf(columns, size);
            }
        }

        @Override
        public int getRowCount()
        {
            return rowCount;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            boolean isOuter = column < outerSize;
            ValueVector vector;
            if (isOuter)
            {
                vector = outer.getColumn(column);
            }
            else
            {
                vector = inner.getColumn(column - outerSize);
            }

            CartesianColumn cc = columns[column];
            if (cc == null)
            {
                cc = new CartesianColumn(vector);
                columns[column] = cc;
            }
            else
            {
                cc.setVector(vector);
            }
            cc.innerRowCount = innerRowCount;
            cc.isOuter = isOuter;

            return cc;
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        static class CartesianColumn extends ValueVectorAdapter
        {
            private int rowCount;
            private boolean isOuter;
            private int innerRowCount;

            CartesianColumn(ValueVector vector)
            {
                super(vector);
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
                    return row / innerRowCount;
                }
                return row % innerRowCount;
            }
        }

        // Validates that schemas are equal. Used in assert to catch errors in tests
        static void assertSchemas(Schema plannedSchema, Schema runtimeSchema, String type)
        {
            assert (plannedSchema.equals(runtimeSchema)) : "Planned " + type
                                                           + " schema should match actual schema. Planned: "
                                                           + plannedSchema.getColumns()
                                                                   .stream()
                                                                   .map(o ->
                                                                   {
                                                                       String str = o.toString();
                                                                       TableSourceReference tableSource = SchemaUtils.getTableSource(o);
                                                                       if (tableSource != null)
                                                                       {
                                                                           str += " (" + tableSource + ")";
                                                                       }
                                                                       return str;
                                                                   })
                                                                   .collect(joining(", "))
                                                           + ", actual: "
                                                           + runtimeSchema.getColumns()
                                                                   .stream()
                                                                   .map(o ->
                                                                   {
                                                                       String str = o.toString();
                                                                       TableSourceReference tableSource = SchemaUtils.getTableSource(o);
                                                                       if (tableSource != null)
                                                                       {
                                                                           str += " (" + tableSource + ")";
                                                                       }
                                                                       return str;
                                                                   })
                                                                   .collect(joining(", "));
        }
    }

    /** Tuple vector that wraps another tuple vector for a single row */
    private static class RowTupleVector implements TupleVector
    {
        private TupleVector wrapped;
        private int row;
        private RowColumnVector[] columns;
        private boolean sealed = false;

        void init(TupleVector wrapped)
        {
            this.wrapped = wrapped;
            int size = wrapped.getSchema()
                    .getSize();
            if (columns == null)
            {
                columns = new RowColumnVector[size];
            }
            else if (columns.length < size)
            {
                columns = Arrays.copyOf(columns, size);
            }
        }

        @Override
        public Schema getSchema()
        {
            return wrapped.getSchema();
        }

        @Override
        public int getRowCount()
        {
            return 1;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            ValueVector wrappedColumn = wrapped.getColumn(column);
            RowColumnVector rcv = columns[column];
            if (rcv == null)
            {
                rcv = new RowColumnVector(wrappedColumn);
                columns[column] = rcv;
            }
            else
            {
                rcv.setVector(wrappedColumn);
            }

            return rcv;
        }

        private class RowColumnVector extends ValueVectorAdapter
        {
            RowColumnVector(ValueVector vector)
            {
                super(vector);
            }

            @Override
            public int size()
            {
                return 1;
            }

            @Override
            protected int getRow(int row)
            {
                if (sealed)
                {
                    throw new IllegalArgumentException("This vector is sealed and should not be used, check implementation of eval and make sure to copy values and not create lazy constructs.");
                }

                // We always return the single wrapped index
                return RowTupleVector.this.row;
            }
        }
    }
}
