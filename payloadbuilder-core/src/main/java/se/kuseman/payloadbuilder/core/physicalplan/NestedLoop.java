package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.ArrayList;
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
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.execution.vector.TupleVectorBuilder;

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

    //@formatter:off
    private NestedLoop(
            int nodeId,
            IPhysicalPlan outer,
            IPhysicalPlan inner,
            BiFunction<TupleVector,
            IExecutionContext, ValueVector> condition,
            String populateAlias,
            Set<Column> outerReferences,
            boolean emitEmptyOuterRows,
            boolean switchedInputs,
            boolean pushOuterReference)
    //@formatter:on
    {
        this.nodeId = nodeId;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "innter");
        this.condition = condition;
        this.populateAlias = populateAlias;
        this.outerReferences = defaultIfNull(outerReferences, emptySet());
        this.pushOuterReference = pushOuterReference
                || this.outerReferences.size() > 0;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.switchedInputs = switchedInputs;

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
    }

    /** Create an inner join. Logical operations: INNER JOIN */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition, String populateAlias,
            boolean pushOuterReference)
    {
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, null, false, false, pushOuterReference);
    }

    /** Create an inner join with no condition and no outer references. Logical operations: CROSS JOIN/CROSS APPLY */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, String populateAlias, boolean switchedInputs)
    {
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, null, false, switchedInputs, false);
    }

    /** Create an inner join with no condition with outer references. Logical operations: CROSS JOIN/CROSS APPLY */
    public static NestedLoop innerJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, String populateAlias)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, outerReferences, false, false, true);
    }

    /** Create a left outer join. Logical operations: LEFT OUTER JOIN */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> condition, String populateAlias,
            boolean pushOuterReference)
    {
        return new NestedLoop(nodeId, outer, inner, condition, populateAlias, null, true, false, pushOuterReference);
    }

    /** Create a left outer join with no condition and no outer references. Logical operations: OUTER APPLY */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, String populateAlias)
    {
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, null, true, false, false);
    }

    /** Create an left outer join with no condition with outer references. Logical operations: OUTER APPLY */
    public static NestedLoop leftJoin(int nodeId, IPhysicalPlan outer, IPhysicalPlan inner, Set<Column> outerReferences, String populateAlias)
    {
        if (outerReferences == null
                || outerReferences.isEmpty())
        {
            throw new IllegalArgumentException("outerReferences must be set");
        }
        return new NestedLoop(nodeId, outer, inner, null, populateAlias, outerReferences, true, false, true);
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
        Schema outerSchema = switchedInputs ? inner.getSchema()
                : outer.getSchema();

        List<Column> columns = new ArrayList<>(outerSchema.getColumns());

        Schema innerSchema = switchedInputs ? outer.getSchema()
                : inner.getSchema();

        if (populateAlias != null)
        {
            // Copy table source from inner schema if any exists
            ColumnReference colRef = SchemaUtils.getColumnReference(innerSchema.getColumns()
                    .get(0));
            colRef = colRef != null ? colRef.rename(populateAlias)
                    : null;

            columns.add(CoreColumn.of(populateAlias, ResolvedType.table(innerSchema), colRef));
        }
        else
        {
            columns.addAll(innerSchema.getColumns());
        }

        return new Schema(columns);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleIterator outerIt = outer.execute(context);

        if (!outerIt.hasNext())
        {
            outerIt.close();
            return TupleIterator.EMPTY;
        }

        final LoopNodeData nodeData = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new LoopNodeData());

        if (condition == null
                && !outerReferences.isEmpty())
        {
            return loopIterator((ExecutionContext) context, outerIt, nodeData);
        }
        else if (populateAlias != null)
        {
            return populatingIterator((ExecutionContext) context, outerIt, nodeData);
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
                        // Outer references are not allowed in inner/left/cross joins so clear
                        ctx.getStatementContext()
                                .setOuterTupleVector(null);
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
                        TupleVector cartesian = VectorUtils.cartesian(currentOuter, currentInner);
                        ValueVector filter = condition.apply(cartesian, context);

                        int cardinality = filter.getCardinality();

                        nodeData.predicateTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                        if (cardinality > 0)
                        {
                            time = System.nanoTime();
                            // Build a matching vector from cartesian and filter
                            TupleVectorBuilder b = new TupleVectorBuilder(executionContext.getBufferAllocator(), cardinality);
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
        else if (obj instanceof NestedLoop)
        {
            NestedLoop that = (NestedLoop) obj;
            return nodeId == that.nodeId
                    && inner.equals(that.inner)
                    && outer.equals(that.outer)
                    && Objects.equals(condition, that.condition)
                    && Objects.equals(populateAlias, that.populateAlias)
                    && outerReferences.equals(that.outerReferences)
                    && pushOuterReference == that.pushOuterReference
                    && emitEmptyOuterRows == that.emitEmptyOuterRows
                    && switchedInputs == that.switchedInputs;
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
    private TupleIterator populatingIterator(final ExecutionContext context, final TupleIterator iterator, final LoopNodeData nodeData)
    {
        return new TupleIterator()
        {
            private TupleVector concatOfInner;
            private TupleVector currentOuter;
            private TupleVector next;
            /** Bit set to keep track of outer matches. Used in left joins to know what outer indices to return */
            private BitSet outerMatches;

            // The inner schema used when emitting empty outer row, will be the plan schema from start
            // but if there are inner matches before the un matched ones we switch
            private Schema innerSchema = inner.getSchema();
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
                // Concat all inner tuples once if we don't need to push outer reference
                if (concatOfInner == null
                        && !pushOuterReference)
                {
                    concatOfInner = PlanUtils.concat(context.getBufferAllocator(), inner.execute(context));
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
                            // INNER/LEFT joins cannot have outer references, clear
                            ctx.getStatementContext()
                                    .setOuterTupleVector(null);
                            ctx.getStatementContext()
                                    .setIndexSeekTupleVector(currentOuter);

                            concatOfInner = PlanUtils.concat(context.getBufferAllocator(), inner.execute(ctx));

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
                        TupleVector cartesian = VectorUtils.cartesian(currentOuter, concatOfInner);
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
                            TupleVectorBuilder b = new TupleVectorBuilder(context.getBufferAllocator(), currentOuter.getRowCount());
                            b.appendPopulate(cartesian, filter, currentOuter, concatOfInner, populateAlias);
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
        };
    }

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
    private TupleIterator loopIterator(final ExecutionContext context, final TupleIterator iterator, final LoopNodeData nodeData)
    {
        final StatementContext statementContext = context.getStatementContext();
        final TupleVector prevOuter = statementContext.getOuterTupleVector();

        return new TupleIterator()
        {
            private TupleVector currentOuter;
            private TupleVector next;
            // The inner schema used when emitting empty outer row, will be the plan schema from start
            // but if there are inner matches before the un matched ones we switch
            private Schema innerSchema = inner.getSchema();
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
                iterator.close();
            }

            private Schema getOuterSchema(TupleVector outer)
            {
                if (prevOuter == null)
                {
                    return outer.getSchema();
                }

                List<Column> columns = new ArrayList<>(prevOuter.getSchema()
                        .getSize()
                        + outer.getSchema()
                                .getSize());
                columns.addAll(prevOuter.getSchema()
                        .getColumns());
                columns.addAll(outer.getSchema()
                        .getColumns());
                return new Schema(columns);
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
                    TupleVectorBuilder builder = null;

                    Schema outerSchema = getOuterSchema(currentOuter);

                    for (int i = 0; i < rowCount; i++)
                    {
                        if (context.getSession()
                                .abortQuery())
                        {
                            return false;
                        }

                        TupleVector outer = new RowTupleVector(currentOuter, i);
                        // Make a copy of the execution context here because we mutate the outer tuple vector
                        // and we risk evaluating against wrong outer tuple otherwise
                        ExecutionContext executionContext = context;
                        if (pushOuterReference)
                        {
                            executionContext = executionContext.copy();
                            executionContext.getStatementContext()
                                    .setOuterTupleVector(concatOuter(prevOuter, outer, outerSchema));
                        }

                        TupleIterator innerIt = inner.execute(executionContext);
                        boolean matched = false;

                        if (populateAlias != null)
                        {
                            TupleVector concatOfInner = PlanUtils.concat(executionContext.getBufferAllocator(), innerIt);
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

                                matched = true;
                                if (builder == null)
                                {
                                    builder = new TupleVectorBuilder(executionContext.getBufferAllocator(), rowCount);
                                }

                                builder.append(VectorUtils.populateCartesian(outer, concatOfInner, populateAlias));

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
                                    if (inner.getRowCount() > 0)
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
                                        matched = true;

                                        if (builder == null)
                                        {
                                            builder = new TupleVectorBuilder(executionContext.getBufferAllocator(), (int) (rowCount * inner.getRowCount() * 1.1));
                                        }

                                        builder.append(VectorUtils.cartesian(outer, inner));

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

                    long time = System.nanoTime();

                    TupleVector unmatchedOuter = emitEmptyOuterRows ? createUnmatchedOuterTuple(context, innerSchema, outerMatches, currentOuter)
                            : null;

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
                    currentOuter = null;

                    nodeData.tupleBuildTime += TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                }

                return true;
            }
        };
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
            // Build an outer vector with un matched rows
            TupleVectorBuilder b = new TupleVectorBuilder(context.getBufferAllocator(), cardinality);
            b.append(outer, outerMatches);
            unmatchedOuter = b.build();
        }

        Schema schema;
        if (populateAlias != null)
        {
            schema = SchemaUtils.populate(outer.getSchema(), populateAlias, innerSchema);
        }
        // If the inner schema is asterisk then just return the outer vector
        else if (SchemaUtils.isAsterisk(innerSchema))
        {
            return outer;
        }
        else
        {
            schema = SchemaUtils.concat(outer.getSchema(), innerSchema);
        }

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

    /** Tuple vector that wraps another tuple vector for a single row */
    private static class RowTupleVector implements TupleVector
    {
        private final TupleVector wrapped;
        private final int row;

        RowTupleVector(TupleVector wrapped, int row)
        {
            this.wrapped = wrapped;
            this.row = row;
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
            return new ValueVectorAdapter(wrapped.getColumn(column))
            {
                @Override
                public int size()
                {
                    return 1;
                }

                @Override
                protected int getRow(int row)
                {
                    // We always return the single wrapped index
                    return RowTupleVector.this.row;
                }
            };
        }
    }

    /**
     * Concats two vectors into one. Takes all columns from the first one and appends to the second one. Requires that both vectors has the same row counts
     */
    private static TupleVector concatOuter(final TupleVector vector1, final TupleVector vector2, final Schema schema)
    {
        if (vector2 == null)
        {
            return vector1;
        }
        else if (vector1 == null)
        {
            return vector2;
        }
        if (vector1.getRowCount() != vector2.getRowCount())
        {
            throw new IllegalArgumentException("Vectors must have equal row counts");
        }
        final int size1 = vector1.getSchema()
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
                return vector1.getRowCount();
            }

            @Override
            public ValueVector getColumn(int column)
            {
                if (column < size1)
                {
                    return vector1.getColumn(column);
                }
                return vector2.getColumn(column - size1);
            }
        };
    }
}
