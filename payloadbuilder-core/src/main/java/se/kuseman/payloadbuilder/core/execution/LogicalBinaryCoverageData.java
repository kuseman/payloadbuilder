package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Per-AND/OR-expression coverage data tracking how many times the condition evaluated to true vs. false. */
public class LogicalBinaryCoverageData extends NodeData
{
    private final String expressionText;
    private long trueHits;
    private long falseHits;
    private final Location location;

    public LogicalBinaryCoverageData(String expressionText, Location location)
    {
        this.expressionText = expressionText;
        this.location = location != null ? location
                : Location.EMPTY;
    }

    public String getExpressionText()
    {
        return expressionText;
    }

    public Location getLocation()
    {
        return location;
    }

    /** Records that the condition evaluated to true for {@code trueCount} rows and false for {@code falseCount} rows. */
    public void recordHits(long trueCount, long falseCount)
    {
        if (trueCount > 0)
        {
            trueHits += trueCount;
        }
        if (falseCount > 0)
        {
            falseHits += falseCount;
        }
    }

    public long getTrueHits()
    {
        return trueHits;
    }

    public long getFalseHits()
    {
        return falseHits;
    }
}
