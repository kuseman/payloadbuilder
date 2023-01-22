package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.MapUtils;
import se.kuseman.payloadbuilder.core.QueryException;

/** Assert plan that asserts a condition on input. Throws exception if not fullfilled */
public class Assert implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final Supplier<Consumer<TupleVector>> predicateSupplier;

    private Assert(int nodeId, IPhysicalPlan input, Supplier<Consumer<TupleVector>> predicateSupplier)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.predicateSupplier = requireNonNull(predicateSupplier, "predicateSupplier");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Assert";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry("Predicate", predicateSupplier.toString()));
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleIterator iterator = input.execute(context);
        final Consumer<TupleVector> predicate = predicateSupplier.get();
        return new TupleIterator()
        {
            @Override
            public TupleVector next()
            {
                if (!iterator.hasNext())
                {
                    throw new NoSuchElementException();
                }

                TupleVector vector = iterator.next();
                predicate.accept(vector);
                return vector;
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public void close()
            {
                iterator.close();
            }
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return singletonList(input);
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
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
        if (obj instanceof Assert)
        {
            Assert that = (Assert) obj;
            return input.equals(that.input)
                    && nodeId == that.nodeId
                    && predicateSupplier.equals(that.predicateSupplier);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Assert (" + nodeId + "), type: " + predicateSupplier;
    }

    /** Create an assert node that assert max rows from input */
    public static Assert maxRowCount(int nodeId, IPhysicalPlan input, int maxRowCount)
    {
        return new Assert(nodeId, input, new MaxRowCountPredicateSupplier(maxRowCount));
    }

    private static class MaxRowCountPredicateSupplier implements Supplier<Consumer<TupleVector>>
    {
        private final int maxRowCount;

        MaxRowCountPredicateSupplier(int maxRowCount)
        {
            this.maxRowCount = maxRowCount;
        }

        @Override
        public Consumer<TupleVector> get()
        {
            return new Consumer<TupleVector>()
            {
                private int rowTotal;

                @Override
                public void accept(TupleVector vector)
                {
                    rowTotal += vector.getRowCount();
                    if (rowTotal > maxRowCount)
                    {
                        throw new QueryException("Query returned too many rows. Expected " + maxRowCount + " row(s) to be returned.");
                    }
                }
            };
        }

        @Override
        public int hashCode()
        {
            return maxRowCount;
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
            if (obj instanceof MaxRowCountPredicateSupplier)
            {
                MaxRowCountPredicateSupplier that = (MaxRowCountPredicateSupplier) obj;
                return maxRowCount == that.maxRowCount;
            }
            return false;
        }

        @Override
        public String toString()
        {
            return "Max row count: " + maxRowCount;
        }
    }
}
