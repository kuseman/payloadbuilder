package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Logical concat operator. Used to concat inputs into one stream. UNION ALL etc. */
public class Concatenation implements ILogicalPlan
{
    private final List<ILogicalPlan> inputs;

    public Concatenation(List<ILogicalPlan> inputs)
    {
        this.inputs = requireNonNull(inputs, "inputs");
        if (inputs.size() <= 1)
        {
            throw new IllegalArgumentException("Size of inputs should be 1 or greater");
        }
        verifySchema();
    }

    @Override
    public Schema getSchema()
    {
        // We use the schema from the first input
        return inputs.get(0)
                .getSchema();
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return inputs;
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
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
        else if (obj instanceof Concatenation)
        {
            Concatenation that = (Concatenation) obj;
            return inputs.equals(that.inputs);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Concatenation";
    }

    private void verifySchema()
    {
        Schema schema = inputs.get(0)
                .getSchema();

        // Asterisk schema on first input then we cannot verify
        if (Schema.EMPTY.equals(schema))
        {
            return;
        }

        int size = inputs.size();
        for (int i = 1; i < size; i++)
        {
            Schema s = inputs.get(i)
                    .getSchema();
            if (schema.getSize() != s.getSize())
            {
                throw new IllegalArgumentException("All inputs for concatenation must equal in column count.");
            }

            int columnCount = s.getSize();
            for (int j = 0; j < columnCount; j++)
            {
                if (!schema.getColumns()
                        .get(j)
                        .getType()
                        .equals(s.getColumns()
                                .get(j)
                                .getType()))
                {
                    throw new IllegalArgumentException("All inputs column types for concatenation must equal.");
                }
            }
        }
    }
}
