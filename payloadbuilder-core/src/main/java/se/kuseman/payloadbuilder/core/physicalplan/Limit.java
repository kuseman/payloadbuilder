package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Limit plan that returns x rows from input */
public class Limit implements IPhysicalPlan
{
    private final int nodeId;
    private final IPhysicalPlan input;
    private final IExpression limitExpression;

    public Limit(int nodeId, IPhysicalPlan input, IExpression limitExpression)
    {
        this.nodeId = nodeId;
        this.input = requireNonNull(input, "input");
        this.limitExpression = requireNonNull(limitExpression, "topElimitExpressionxpression");
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public String getName()
    {
        return "Limit";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(input.getSchema()));
        properties.put("Value", limitExpression.toString());
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        ValueVector result = limitExpression.eval(TupleVector.CONSTANT, context);
        if (result.size() == 0)
        {
            throw new QueryException("Limit expression returned no rows");
        }
        final int topCount = result.getInt(0);
        final TupleIterator it = input.execute(context);
        return new TupleIterator()
        {
            TupleVector next;
            int count;

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
                it.close();
            }

            private boolean setNext()
            {
                if (next != null)
                {
                    return true;
                }

                // We reached top count or no more batches
                if (count >= topCount
                        || !it.hasNext())
                {
                    return false;
                }

                TupleVector vector = it.next();

                int maxRows = Math.min(topCount - count, vector.getRowCount());
                count += maxRows;
                next = new MaxTupleVector(vector, maxRows);
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
        return input.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Limit)
        {
            Limit that = (Limit) obj;
            return input.equals(that.input)
                    && limitExpression.equals(that.limitExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "LIMIT " + limitExpression.toVerboseString();
    }

    /** Tuple vector decoration that returns max rows */
    private static class MaxTupleVector implements TupleVector
    {
        private final TupleVector wrapped;
        private final int maxRows;

        MaxTupleVector(TupleVector wrapped, int maxRows)
        {
            this.wrapped = wrapped;
            this.maxRows = maxRows;
        }

        @Override
        public int getRowCount()
        {
            return maxRows;
        }

        @Override
        public ValueVector getColumn(int column)
        {
            final ValueVector vv = wrapped.getColumn(column);
            return new ValueVectorAdapter(vv)
            {
                @Override
                public int size()
                {
                    return maxRows;
                }
            };
        }

        @Override
        public Schema getSchema()
        {
            return wrapped.getSchema();
        }
    }
}
