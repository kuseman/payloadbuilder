package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntHash.Strategy;
import it.unimi.dsi.fastutil.ints.IntList;

/** Plan for a group by. Aggregates input with hashing of provided expressions and outputs a projected result. */
public class HashAggregate implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final List<IAggregateExpression> projectionExpressions;
    private final List<IExpression> aggregateExpressions;
    private final boolean hasAsteriskProjections;

    public HashAggregate(int nodeId, IPhysicalPlan input, List<IExpression> aggregateExpressions, List<IAggregateExpression> projectionExpressions)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.projectionExpressions = requireNonNull(projectionExpressions, "projectionExpressions");
        this.aggregateExpressions = requireNonNull(aggregateExpressions, "aggregateExpressions");
        this.hasAsteriskProjections = projectionExpressions.stream()
                .anyMatch(e ->
                {
                    if (e instanceof HasColumnReference)
                    {
                        ColumnReference colRef = ((HasColumnReference) e).getColumnReference();
                        if (colRef != null
                                && colRef.isAsterisk())
                        {
                            return true;
                        }
                    }

                    return false;
                });

        if ((aggregateExpressions.isEmpty()
                && !projectionExpressions.isEmpty())
                || (!aggregateExpressions.isEmpty()
                        && projectionExpressions.isEmpty()))
        {
            throw new IllegalArgumentException("Both aggregate and projection expressions must be empty or not empty");
        }
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return aggregateExpressions.isEmpty() ? "Hash Distinct"
                : "Hash Aggregate";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        long hashTime = data != null ? data.hashTime
                : 0;

        Schema schema = getSchema();
        if (aggregateExpressions.isEmpty())
        {
            return ofEntries(true, entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(schema)), entry("Hash time", DurationFormatUtils.formatDurationHMS(hashTime)));
        }

        return ofEntries(true, entry("Group By", aggregateExpressions.stream()
                .map(e -> e.toString())
                .collect(toList())), entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(schema)), entry(IDatasource.DEFINED_VALUES,
                        projectionExpressions.stream()
                                .filter(e -> e instanceof AggregateWrapperExpression
                                        && ((AggregateWrapperExpression) e).isInternal())
                                .map(e ->
                                {
                                    IExpression ee = ((AggregateWrapperExpression) e).getExpression();
                                    String alias;
                                    if (ee instanceof AliasExpression)
                                    {
                                        alias = ((AliasExpression) ee).getAliasString();
                                        ee = ((AliasExpression) ee).getExpression();
                                    }
                                    else if (ee instanceof HasAlias)
                                    {
                                        alias = ((HasAlias) ee).getAlias()
                                                .getAlias();
                                    }
                                    else
                                    {
                                        alias = ee.toString();
                                    }

                                    return alias + ": " + ee.toString();
                                })
                                .collect(joining(", "))),
                entry("Hash Time", DurationFormatUtils.formatDurationHMS(hashTime)));
    }

    @Override
    public Schema getSchema()
    {
        if (projectionExpressions.isEmpty())
        {
            return input.getSchema();
        }
        return ProjectionUtils.createSchema(Schema.EMPTY, projectionExpressions, false, true);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // CSOFF
        TupleVector outerTupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();
        // CSON
        TupleIterator iterator = input.execute(context);
        final TupleVector all = PlanUtils.concat(new BufferAllocator(), iterator);
        if (all.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        // CSOFF
        Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, () -> new Data());
        long start = System.nanoTime();
        // CSON
        Schema outerSchema = null;
        ValueVector[] vectors;
        if (!aggregateExpressions.isEmpty())
        {
            outerSchema = outerTupleVector != null ? outerTupleVector.getSchema()
                    : null;

            List<? extends IExpression> actualExpressions = aggregateExpressions;
            int size = actualExpressions.size();
            vectors = new ValueVector[size];

            for (int i = 0; i < size; i++)
            {
                vectors[i] = actualExpressions.get(i)
                        .eval(all, context);
            }
        }
        else
        {
            // Aggregate whole input
            int size = all.getSchema()
                    .getSize();
            vectors = new ValueVector[size];

            for (int i = 0; i < size; i++)
            {
                vectors[i] = all.getColumn(i);
            }
        }

        // We use a map-impl. with a custom hash/equals strategy to be able to eliminate
        // the need of immutable keys which would allocate a lot (that would be needed if using JDK HashMap).
        // We are only interested in grouping indices and no lookups on keys will be made
        Int2ObjectMap<IntList> table = new Int2ObjectOpenCustomHashMap<IntList>(new Strategy()
        {
            @Override
            public int hashCode(int e)
            {
                return VectorUtils.hash(vectors, e);
            }

            @Override
            public boolean equals(int a, int b)
            {
                return VectorUtils.equals(vectors, a, b);
            }
        });

        // Fill table and group rows
        int rowCount = all.getRowCount();
        for (int i = 0; i < rowCount; i++)
        {
            // // TODO: estimate a good count of each group to avoid allocations of new int arrays
            table.computeIfAbsent(i, k -> new IntArrayList(10))
                    .add(i);
        }

        final List<IntList> groups = new ArrayList<>(table.values());
        final int groupSize = groups.size();

        Schema s;
        List<ValueVector> r;

        if (!projectionExpressions.isEmpty())
        {
            // Create a wrapper value vector that returns a tuple vector for each group
            GroupsValueVector valueVector = new GroupsValueVector(all, groups);

            List<? extends IExpression> actualExpressions = hasAsteriskProjections ? ProjectionUtils.expandExpressions(projectionExpressions, outerSchema, all.getSchema())
                    : projectionExpressions;

            int projectionSize = actualExpressions.size();
            r = new ArrayList<>(projectionSize);

            // Evaluate each projection
            for (int i = 0; i < projectionSize; i++)
            {
                r.add(((IAggregateExpression) actualExpressions.get(i)).eval(valueVector, context));
            }

            // Create the actual schema from the expressions
            // TODO: this should done only once in planning if the input schema is static
            s = ProjectionUtils.createSchema(all.getSchema(), actualExpressions, false, true);
        }
        else
        {
            // Distinct output from input
            s = all.getSchema();
            int size = s.getSize();
            r = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
            {
                r.add(new ValueVectorAdapter(all.getColumn(i))
                {
                    @Override
                    public int size()
                    {
                        return groupSize;
                    };

                    @Override
                    protected int getRow(int row)
                    {
                        // Return the first row from each group
                        return groups.get(row)
                                .getInt(0);
                    };
                });
            }
        }

        data.hashTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        final Schema schema = s;
        final List<ValueVector> result = r;

        // We only have one resulting vector from an aggregate
        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return groupSize;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return result.get(column);
            }
        });
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(input);
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
        else if (obj instanceof HashAggregate)
        {
            HashAggregate that = (HashAggregate) obj;
            return nodeId == that.nodeId
                    && input.equals(that.input)
                    && aggregateExpressions.equals(that.aggregateExpressions)
                    && projectionExpressions.equals(that.projectionExpressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (aggregateExpressions.isEmpty())
        {
            return "Distinct (" + nodeId + ")";
        }

        return "HashAggregate (" + nodeId + "), on: " + aggregateExpressions + ", projection: " + projectionExpressions;
    }

    /** Node data */
    static class Data extends NodeData
    {
        long hashTime;
    }

    /** Value vector that has all grouped rows as separate TupleVector's used by {@link IAggregateFunction}'s to evaluate result */
    private static class GroupsValueVector implements ValueVector
    {
        private final TupleVector vector;
        private final List<IntList> groups;

        GroupsValueVector(TupleVector vector, List<IntList> groups)
        {
            this.vector = vector;
            this.groups = groups;
        }

        @Override
        public int size()
        {
            return groups.size();
        }

        @Override
        public ResolvedType type()
        {
            return ResolvedType.table(vector.getSchema());
        }

        @Override
        public boolean isNull(int row)
        {
            return false;
        }

        @Override
        public TupleVector getTable(int row)
        {
            // Return a tuple vector for row's group
            final IntList group = groups.get(row);
            return new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    // The schema is the same as the input to HashAggregate
                    return vector.getSchema();
                }

                @Override
                public int getRowCount()
                {
                    return group.size();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector vv = vector.getColumn(column);
                    return new ValueVectorAdapter(vv)
                    {
                        @Override
                        public int size()
                        {
                            return group.size();
                        }

                        @Override
                        protected int getRow(int row)
                        {
                            // Return the group index from the "big" vector
                            return group.getInt(row);
                        }
                    };
                }
            };
        }
    }
}
