package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Concat inputs into a single output stream. UNION ALL etc. */
public class Concatenation implements IPhysicalPlan
{
    private final int nodeId;
    private final List<IPhysicalPlan> inputs;

    public Concatenation(int nodeId, List<IPhysicalPlan> inputs)
    {
        this.nodeId = nodeId;
        this.inputs = requireNonNull(inputs, "inputs");
        if (inputs.size() <= 1)
        {
            throw new IllegalArgumentException("Size of inputs should be 1 or greater");
        }
    }

    @Override
    public int getNodeId()
    {
        return nodeId;
    }

    @Override
    public Schema getSchema()
    {
        // We use the schema from the first input
        return inputs.get(0)
                .getSchema();
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        return new TupleIterator()
        {
            private int inputIndex = 0;
            private TupleIterator inputIterator;
            private TupleVector next;
            private Schema schema = null;

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                final TupleVector result = next;
                // Use schema from first input
                if (schema == null)
                {
                    // TODO: might be weird since we will also get the columnreferences which if wrong for other vectors than form the first input
                    schema = result.getSchema();
                }
                next = null;
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
                        return result.getRowCount();
                    }

                    @Override
                    public ValueVector getColumn(int column)
                    {
                        return result.getColumn(column);
                    }
                };
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public void close()
            {
                if (inputIterator != null)
                {
                    inputIterator.close();
                }
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    // Move to next input
                    if (inputIterator == null)
                    {
                        if (inputIndex >= inputs.size())
                        {
                            return false;
                        }
                        inputIterator = inputs.get(inputIndex)
                                .execute(context);
                        inputIndex++;
                    }
                    else if (!inputIterator.hasNext())
                    {
                        inputIterator.close();
                        inputIterator = null;
                        continue;
                    }

                    next = inputIterator.next();
                }
                return true;
            }
        };
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return inputs;
    }

    @Override
    public int hashCode()
    {
        return inputs.hashCode();
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
        else if (obj instanceof Concatenation that)
        {
            return inputs.equals(that.inputs);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Concatenation";
    }
}
