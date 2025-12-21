package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;

/** Concat inputs into a single output stream. UNION ALL etc. */
public class Concatenation implements IPhysicalPlan
{
    private final int nodeId;
    private final Schema plannedSchema;
    private final List<IPhysicalPlan> inputs;

    public Concatenation(int nodeId, Schema schema, List<IPhysicalPlan> inputs)
    {
        this.nodeId = nodeId;
        this.plannedSchema = requireNonNull(schema, "schema");
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

    public List<IPhysicalPlan> getInputs()
    {
        return inputs;
    }

    @Override
    public String getName()
    {
        return "Concatenation";
    }

    @Override
    public <T, C> T accept(IPhysicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Schema getSchema()
    {
        return plannedSchema;
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
                    schema = SchemaUtils.isAsterisk(plannedSchema) ? result.getSchema()
                            : plannedSchema;

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
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return Map.of(IDatasource.OUTPUT, DescribeUtils.getOutputColumns(plannedSchema));
    }

    @Override
    public List<IPhysicalPlan> getChildren()
    {
        return inputs;
    }

    @Override
    public List<? extends DescribableNode> getChildNodes()
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
            return plannedSchema.equals(that.plannedSchema)
                    && inputs.equals(that.inputs);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Concatenation";
    }
}
