package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

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

        // if (pushOuterReference
        // && condition != null)
        // {
        // throw new IllegalArgumentException("When having outer reference there should be no predicate.");
        // }
        if (switchedInputs
                && (condition != null
                        || populateAlias != null))
        {
            throw new IllegalArgumentException("Switched inputs are only supported for non conditional loops.");
        }
        else if (pushOuterReference
                && condition != null
                && populateAlias != null)
        {
            throw new UnsupportedOperationException("Push outer reference when populate joining is unsupportd");
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
            ColumnReference colRef = innerSchema.getColumns()
                    .get(0)
                    .getColumnReference();
            colRef = colRef != null ? colRef.rename(populateAlias)
                    : null;

            columns.add(new Column(populateAlias, ResolvedType.tupleVector(innerSchema), colRef));
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

        if (condition == null
                && !outerReferences.isEmpty())
        {
            return loopIterator(context, outerIt);
        }
        else if (populateAlias != null)
        {
            return populatingIterator(context, outerIt);
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
            private boolean innerSchemaAsterisk = innerSchema.isAsterisk();

            @Override
            public TupleVector next()
            {
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
                    if (innerIt == null
                            && !outerIt.hasNext())
                    {
                        // Done
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = outerIt.next();
                        if (emitEmptyOuterRows)
                        {
                            outerMatches = new BitSet(currentOuter.getRowCount());
                        }
                    }
                    if (innerIt == null)
                    {
                        ExecutionContext executionContext = (ExecutionContext) context;
                        if (pushOuterReference)
                        {
                            // Make a copy of the execution context here because we mutable the outer tuple vector
                            // and we risk evaluating against wrong tuple otherwise if expressions are lazy etc.
                            ((ExecutionContext) context).copy();
                            executionContext.getStatementContext()
                                    .setOuterTupleVector(currentOuter);
                        }

                        innerIt = inner.execute(executionContext);
                        continue;
                    }
                    else if (!innerIt.hasNext())
                    {
                        next = createUnmatchedOuterTuple(innerSchema, outerMatches, currentOuter);
                        innerIt.close();
                        innerIt = null;
                        currentOuter = null;
                        continue;
                    }
                    currentInner = innerIt.next();
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
                            next = JoinUtils.crossJoin(currentInner, currentOuter, populateAlias);
                        }
                        else
                        {
                            next = JoinUtils.crossJoin(currentOuter, currentInner, populateAlias);
                        }
                        // Mark all outer rows as matched since we have no predicate
                        if (emitEmptyOuterRows)
                        {
                            outerMatches.flip(0, currentOuter.getRowCount());
                        }
                    }
                    else
                    {
                        // First construct a cartesian tuple vector that will be the one we run the predicate against
                        TupleVector cartesian = VectorUtils.cartesian(currentOuter, currentInner);
                        ValueVector filter = condition.apply(cartesian, context);

                        if (filter.getCardinality() == 0)
                        {
                            continue;
                        }

                        // Mark matched outer rows
                        if (emitEmptyOuterRows)
                        {
                            markMatchedRows(outerMatches, filter, currentInner);
                        }

                        next = new PredicatedTupleVector(cartesian, filter);
                    }
                    continue;
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
    private TupleIterator populatingIterator(final IExecutionContext context, final TupleIterator iterator)
    {
        return new TupleIterator()
        {
            private TupleVector concatOfInner;
            private TupleVector currentOuter;
            private TupleVector next;
            private TupleVector unmatchedOuter;
            /** Bit set to keep track of outer matches. Used in left joins to know what outer indices to return */
            private BitSet outerMatches;

            // The inner schema used when emitting empty outer row, will be the plan schema from start
            // but if there are inner matches before the un matched ones we switch
            private Schema innerSchema = inner.getSchema();
            private boolean innerSchemaAsterisk = innerSchema.isAsterisk();

            @Override
            public TupleVector next()
            {
                TupleVector result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                // Concat all inner tuples once
                if (concatOfInner == null)
                {
                    TupleIterator it = inner.execute(context);
                    concatOfInner = PlanUtils.concat(it);
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
                    // If we have an un matched outer in last iteration, return that one
                    if (unmatchedOuter != null)
                    {
                        next = unmatchedOuter;
                        unmatchedOuter = null;
                        return true;
                    }

                    if (!iterator.hasNext())
                    {
                        return false;
                    }
                    else if (currentOuter == null)
                    {
                        currentOuter = iterator.next();
                        if (emitEmptyOuterRows)
                        {
                            outerMatches = new BitSet(currentOuter.getRowCount());
                        }
                    }

                    if (condition == null)
                    {
                        next = JoinUtils.crossJoin(currentOuter, concatOfInner, populateAlias);
                        // Mark all outer rows as matched since we have no predicate
                        if (emitEmptyOuterRows)
                        {
                            outerMatches.flip(0, currentOuter.getRowCount());
                        }
                    }
                    else
                    {
                        // First construct a cartesian tuple vector that will be the one we run the predicate against
                        TupleVector cartesian = VectorUtils.cartesian(currentOuter, concatOfInner);
                        ValueVector filter = condition.apply(cartesian, context);

                        if (filter.getCardinality() == 0)
                        {
                            next = createUnmatchedOuterTuple(innerSchema, outerMatches, currentOuter);
                            continue;
                        }

                        // Mark matched outer rows
                        if (emitEmptyOuterRows)
                        {
                            markMatchedRows(outerMatches, filter, concatOfInner);
                        }

                        next = new PredicatedPopulatedTupleVector(currentOuter, concatOfInner, filter, populateAlias);
                    }
                    unmatchedOuter = createUnmatchedOuterTuple(innerSchema, outerMatches, currentOuter);
                    currentOuter = null;
                    continue;
                }

                return true;
            }
        };
    }

    /**
     * Create an loop iterator where iterator row by row and not vector by vector.
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
    private TupleIterator loopIterator(final IExecutionContext context, final TupleIterator iterator)
    {
        final StatementContext statementContext = (StatementContext) context.getStatementContext();
        final TupleVector prevOuter = statementContext.getOuterTupleVector();

        return new TupleIterator()
        {
            private TupleVector currentOuter;
            private TupleVector next;

            @Override
            public TupleVector next()
            {
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

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!iterator.hasNext())
                    {
                        return false;
                    }
                    else if (currentOuter == null)
                    {
                        currentOuter = iterator.next();
                    }

                    int rowCount = currentOuter.getRowCount();
                    List<TupleVector> vectors = new ArrayList<>(rowCount);

                    for (int i = 0; i < rowCount; i++)
                    {
                        TupleVector outer = VectorUtils.concat(prevOuter, new RowTupleVector(currentOuter, i));
                        // Make a copy of the execution context here because we mutable the outer tuple vector
                        // and we risk evaluating against wrong tuple otherwise
                        ExecutionContext executionContext = ((ExecutionContext) context).copy();
                        executionContext.getStatementContext()
                                .setOuterTupleVector(outer);

                        boolean match = false;

                        // Populate => join current outer with all inner tuples
                        if (populateAlias != null)
                        {
                            TupleIterator innerIt = inner.execute(executionContext);
                            TupleVector innerVectors = PlanUtils.concat(innerIt);

                            if (innerVectors.getRowCount() > 0)
                            {
                                match = true;
                                TupleVector result = JoinUtils.crossJoin(outer, innerVectors, populateAlias);
                                vectors.add(result);
                            }
                        }
                        else
                        {
                            // TODO: this might not be that good to collect all results before streaming out result
                            // Can be memory heavy
                            TupleIterator innerIt = inner.execute(executionContext);
                            try
                            {
                                while (innerIt.hasNext())
                                {
                                    TupleVector inner = innerIt.next();
                                    if (inner.getRowCount() > 0)
                                    {
                                        match = true;
                                        TupleVector result = JoinUtils.crossJoin(outer, inner, populateAlias);
                                        vectors.add(result);
                                    }
                                }
                            }
                            finally
                            {
                                innerIt.close();
                            }
                        }
                        if (!match
                                && emitEmptyOuterRows)
                        {
                            vectors.add(outer);
                        }
                    }
                    if (vectors.isEmpty())
                    {
                        continue;
                    }

                    currentOuter = null;
                    next = VectorUtils.merge(vectors);
                    break;
                }

                return true;
            }
        };
    }

    /** Creates a tuple vector with non matched outer rows. Used i left joins */
    private TupleVector createUnmatchedOuterTuple(Schema innerSchema, BitSet outerMatches, TupleVector outer)
    {
        // No left/outer, return null
        if (!emitEmptyOuterRows)
        {
            return null;
        }

        // Flip all bits then all set bits are the unmatched outer rows
        outerMatches.flip(0, outer.getRowCount());

        if (outerMatches.cardinality() > 0)
        {
            // When we have a populate alia we can always construct a non matched schema
            if (populateAlias != null)
            {
                Schema schema = outer.getSchema()
                        .populate(populateAlias, innerSchema);
                return new PredicatedTupleVector(outer, schema, new BitSetVector(outer.getRowCount(), outerMatches));
            }

            // If the inner schema is asterisk then just return the outer vector
            if (innerSchema.isAsterisk())
            {
                return new PredicatedTupleVector(outer, new BitSetVector(outer.getRowCount(), outerMatches));
            }

            // Else we return the outer but with a correct schema according to outer/inner that will yield
            // null values for inner rows
            Schema schema = Schema.concat(outer.getSchema(), innerSchema);
            return new PredicatedTupleVector(outer, schema, new BitSetVector(outer.getRowCount(), outerMatches));
        }

        return null;
    }

    private void markMatchedRows(BitSet outerMatches, ValueVector filter, TupleVector inner)
    {
        int innerRowCount = inner.getRowCount();
        int size = filter.size();
        for (int i = 0; i < size; i++)
        {
            int outerIndex = i / innerRowCount;
            boolean match = filter.getPredicateBoolean(i);
            if (match)
            {
                outerMatches.set(outerIndex);
            }
        }
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
}
