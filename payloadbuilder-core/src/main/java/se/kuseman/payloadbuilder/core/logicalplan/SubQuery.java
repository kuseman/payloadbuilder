package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference.Type;
import se.kuseman.payloadbuilder.core.parser.Location;

/**
 * Sub query table source.
 * 
 * <pre>
 * 
 *  Select *
 *  from
 *  (
 *      select *
 *      from table
 *  ) x
 * 
 * </pre>
 */
public class SubQuery extends TableSource
{
    private final ILogicalPlan input;
    private final Location location;

    public SubQuery(ILogicalPlan input, TableSourceReference tableSource, Location location)
    {
        super(tableSource);
        this.input = requireNonNull(input, "input");
        this.location = location;
        if (tableSource.getType() != Type.SUBQUERY)
        {
            throw new IllegalArgumentException("Wrong table source type");
        }
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public Schema getSchema()
    {
        // Schema of a sub query is the sub queries schema
        return input.getSchema();
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
        else if (obj instanceof SubQuery that)
        {
            return super.equals(that)
                    && input.equals(that.input);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "SubQuery (" + getAlias() + ")";
    }
}
