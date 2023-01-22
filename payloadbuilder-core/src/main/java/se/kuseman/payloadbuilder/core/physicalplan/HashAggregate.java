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

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.StringUtils;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
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
    private final boolean hasAsteriskAggregations;

    public HashAggregate(int nodeId, IPhysicalPlan input, List<IExpression> aggregateExpressions, List<IAggregateExpression> projectionExpressions)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.projectionExpressions = requireNonNull(projectionExpressions, "projectionExpressions");
        this.aggregateExpressions = requireNonNull(aggregateExpressions, "aggregateExpressions");
        this.hasAsteriskProjections = projectionExpressions.stream()
                .anyMatch(e -> e.getColumnReference() != null
                        && e.getColumnReference()
                                .isAsterisk());
        this.hasAsteriskAggregations = aggregateExpressions.stream()
                .anyMatch(e -> e.getColumnReference() != null
                        && e.getColumnReference()
                                .isAsterisk());
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Hash Aggregate";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry("Group By", aggregateExpressions.stream()
                .map(e -> e.toString())
                .collect(toList())), entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(createSchema())), entry(IDatasource.DEFINED_VALUES,
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
                                .collect(joining(", "))));
    }

    @Override
    public Schema getSchema()
    {
        if (hasAsteriskProjections)
        {
            return Schema.EMPTY;
        }
        return ProjectionUtils.createSchema(Schema.EMPTY, projectionExpressions, false);
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        // CSOFF
        TupleVector outerTupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();
        // CSON
        TupleIterator iterator = input.execute(context);
        final TupleVector all = PlanUtils.concat(iterator);
        if (all.getRowCount() == 0)
        {
            return TupleIterator.EMPTY;
        }

        Schema outerSchema = outerTupleVector != null ? outerTupleVector.getSchema()
                : null;

        List<? extends IExpression> actualExpressions = hasAsteriskAggregations ? ProjectionUtils.expandExpressions(aggregateExpressions, outerSchema, all.getSchema())
                : aggregateExpressions;

        int size = actualExpressions.size();
        ValueVector[] vectors = new ValueVector[size];

        for (int i = 0; i < size; i++)
        {
            vectors[i] = actualExpressions.get(i)
                    .eval(all, context);
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

        int rowCount = all.getRowCount();
        for (int i = 0; i < rowCount; i++)
        {
            // // TODO: estimate a good count of each group to avoid allocations of new int arrays
            table.computeIfAbsent(i, k -> new IntArrayList(10))
                    .add(i);
        }

        List<IntList> groups = new ArrayList<>(table.values());

        actualExpressions = hasAsteriskProjections ? ProjectionUtils.expandExpressions(projectionExpressions, outerSchema, all.getSchema())
                : projectionExpressions;

        int projectionSize = actualExpressions.size();
        List<ValueVector> result = new ArrayList<>(projectionSize);

        // Create a wrapper value vector that returns a tuple vector for each group
        GroupsValueVector valueVector = new GroupsValueVector(all, groups);

        // Evaluate each projection
        for (int i = 0; i < projectionSize; i++)
        {
            result.add(((IAggregateExpression) actualExpressions.get(i)).eval(valueVector, context));
        }

        // Create the actual schema from the expressions
        // TODO: this should done only once in planning if the input schema is static
        final Schema schema = ProjectionUtils.createSchema(all.getSchema(), actualExpressions, false);

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
                return groups.size();
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
        return "HashAggregate (" + nodeId + "), on: " + aggregateExpressions + ", projection: " + projectionExpressions;
    }

    private Schema createSchema()
    {
        int size = projectionExpressions.size();
        List<Column> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            IAggregateExpression e = projectionExpressions.get(i);
            String name = "";
            String outputName = "";
            if (e instanceof HasAlias)
            {
                HasAlias.Alias alias = ((HasAlias) e).getAlias();
                name = alias.getAlias();
                outputName = alias.getOutputAlias();
            }

            if (StringUtils.isBlank(name))
            {
                outputName = e.toString();
            }

            ResolvedType type = e.getType();
            ColumnReference colRef = e.getColumnReference();
            columns.add(new Column(name, outputName, type, colRef, e.isInternal()));
        }
        return new Schema(columns);
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
            return ResolvedType.tupleVector(vector.getSchema());
        }

        @Override
        public boolean isNull(int row)
        {
            return false;
        }

        @Override
        public boolean isNullable()
        {
            return false;
        }

        @Override
        public Object getValue(int row)
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
