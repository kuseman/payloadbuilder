package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.parser.Location;

/** User defined function that aggregates input into a scalar value */
public class OperatorFunctionScan implements ILogicalPlan
{
    private final Schema schema;
    private final ILogicalPlan input;
    private final String catalogAlias;
    private final String function;
    private final Location location;

    public OperatorFunctionScan(Schema schema, ILogicalPlan input, String catalogAlias, String function, Location location)
    {
        this.schema = requireNonNull(schema, "schema");
        this.input = requireNonNull(input, "input");
        this.catalogAlias = Objects.toString(catalogAlias, "");
        this.function = requireNonNull(function, "function");
        this.location = location;

        if (schema.getSize() != 1)
        {
            throw new IllegalArgumentException("Schema for a tuple function must have a single column");
        }
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getFunction()
    {
        return function;
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
        return singletonList(input);
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return input.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof OperatorFunctionScan that)
        {
            return input.equals(that.input)
                    && catalogAlias.equals(that.catalogAlias)
                    && function.equals(that.function)
                    && schema.equals(that.schema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "OperatorFunction: " + (!"".equals(catalogAlias) ? catalogAlias + "#"
                : "")
               + function
               + " ("
               + "schema: "
               + schema
               + ")";
    }
}
