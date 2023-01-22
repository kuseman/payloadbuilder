package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Filter plan. Evaluates a predicate on input and return matched rows */
public class Filter implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final BiFunction<TupleVector, IExecutionContext, ValueVector> predicate;

    public Filter(int nodeId, IPhysicalPlan input, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.predicate = requireNonNull(predicate, "predicate");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public String getName()
    {
        return "Filter";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        //@formatter:off
        return ofEntries(true, 
                entry(IDatasource.PREDICATE, predicate.toString()), 
                entry(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(getSchema())));
        //@formatter:on
    }

    @Override
    public Schema getSchema()
    {
        // A filter don't alter the input schema
        return input.getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        final TupleIterator iterator = input.execute(context);

        return new TupleIterator()
        {
            private TupleVector next;

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

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!iterator.hasNext())
                    {
                        return false;
                    }

                    TupleVector vector = iterator.next();
                    ValueVector result = predicate.apply(vector, context);

                    // No matched rows
                    if (result.getCardinality() == 0)
                    {
                        continue;
                    }

                    next = new PredicatedTupleVector(vector, result);
                }
                return true;
            }
        };
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
        if (obj instanceof Filter)
        {
            Filter that = (Filter) obj;
            return input.equals(that.input)
                    && nodeId == that.nodeId
                    && predicate.equals(that.predicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Filter (" + nodeId + "): " + predicate.toString();
    }
}
