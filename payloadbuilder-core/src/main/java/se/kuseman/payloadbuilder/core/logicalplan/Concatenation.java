package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Logical concat operator. Used to concat inputs into one stream.
 * Used as UNION ALL, VALUES row constructor etc.
 */
public class Concatenation implements ILogicalPlan
{
    private final List<ILogicalPlan> inputs;
    private final Schema schema;
    private final Location location;

    public Concatenation(List<ILogicalPlan> inputs, Location location)
    {
        this.inputs = requireNonNull(inputs, "inputs");
        if (inputs.size() <= 1)
        {
            throw new IllegalArgumentException("Size of inputs should be 1 or greater");
        }
        this.location = location;
        this.schema = verifySchema();
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
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

    private Schema verifySchema()
    {
        // Any asterisk schema then simply return an asterisk schema since we cannot verify anything
        if (inputs.stream()
                .anyMatch(i -> SchemaUtils.isAsterisk(i.getSchema())))
        {
            return Schema.EMPTY;
        }

        Schema schema = inputs.get(0)
                .getSchema();

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

        return schema;
    }
}
