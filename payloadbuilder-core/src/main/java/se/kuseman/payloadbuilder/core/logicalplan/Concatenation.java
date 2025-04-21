package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.stream.IntStream;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/**
 * Logical concat operator. Used to concat inputs into one stream. Used as UNION ALL, VALUES row constructor etc.
 */
public class Concatenation implements ILogicalPlan
{
    private final List<ILogicalPlan> inputs;
    private final List<String> columnNames;
    private final Schema schema;
    private final Location location;

    public Concatenation(List<String> columnNames, List<ILogicalPlan> inputs, Location location)
    {
        this.columnNames = requireNonNull(columnNames, "columnNames");
        this.inputs = requireNonNull(inputs, "inputs");
        if (inputs.size() <= 1)
        {
            throw new IllegalArgumentException("Number of inputs must be greater than 1");
        }
        this.location = location;
        this.schema = verifySchema();
    }

    public List<String> getColumnNames()
    {
        return columnNames;
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
        else if (obj instanceof Concatenation that)
        {
            return columnNames.equals(that.columnNames)
                    && inputs.equals(that.inputs);
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
        // Asterisks schemas among imput
        boolean diffsAllowed = columnNames.isEmpty();

        Schema result = inputs.get(0)
                .getSchema();

        int size = inputs.size();
        for (int i = 1; i < size; i++)
        {
            Schema s = inputs.get(i)
                    .getSchema();

            if (result.getSize() != s.getSize())
            {
                if (diffsAllowed)
                {
                    return Schema.EMPTY;
                }

                throw new IllegalArgumentException("All inputs for concatenation must equal in column count.");
            }

            int columnCount = s.getSize();
            for (int j = 0; j < columnCount; j++)
            {
                if (!result.getColumns()
                        .get(j)
                        .getType()
                        .equals(s.getColumns()
                                .get(j)
                                .getType()))
                {
                    if (diffsAllowed)
                    {
                        return Schema.EMPTY;
                    }
                    throw new IllegalArgumentException("All inputs column types for concatenation must equal.");
                }
            }
        }

        if (!columnNames.isEmpty()
                && result.getSize() != columnNames.size())
        {
            throw new ParseException("Column alias count must equal the inputs column count", location);
        }

        // TODO: Construct schema from inputs with type precedence in mind
        // column names is either provided or from first input

        if (!columnNames.isEmpty())
        {
            return new Schema(IntStream.range(0, result.getSize())
                    .mapToObj(i -> new Column(columnNames.get(i), result.getColumns()
                            .get(i)
                            .getType()))
                    .toList());
        }

        return result;
    }
}
