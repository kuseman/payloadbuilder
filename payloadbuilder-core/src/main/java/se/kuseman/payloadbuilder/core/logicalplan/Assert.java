package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Logical plan for an assert that assert a predicate from input and throws if not full filled */
abstract class Assert implements ILogicalPlan
{
    protected final ILogicalPlan input;

    public Assert(ILogicalPlan input)
    {
        this.input = requireNonNull(input, "input");
    }

    public ILogicalPlan getInput()
    {
        return input;
    }

    @Override
    public Schema getSchema()
    {
        return input.getSchema();
    }

    @Override
    public List<ILogicalPlan> getChildren()
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
            return input.equals(that.input);
        }
        return false;
    }
}
