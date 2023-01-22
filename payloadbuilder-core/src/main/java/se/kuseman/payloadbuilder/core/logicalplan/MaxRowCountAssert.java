package se.kuseman.payloadbuilder.core.logicalplan;

/** Max row count assert rule */
public class MaxRowCountAssert extends Assert
{
    private final int maxRowCount;

    public MaxRowCountAssert(ILogicalPlan input, int maxRowCount)
    {
        super(input);
        this.maxRowCount = maxRowCount;
    }

    public int getMaxRowCount()
    {
        return maxRowCount;
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
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        if (obj instanceof MaxRowCountAssert)
        {
            MaxRowCountAssert that = (MaxRowCountAssert) obj;
            return super.equals(obj)
                    && maxRowCount == that.maxRowCount;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Assert max row count: " + maxRowCount;
    }
}
