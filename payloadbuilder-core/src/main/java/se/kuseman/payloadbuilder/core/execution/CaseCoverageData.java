package se.kuseman.payloadbuilder.core.execution;

import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Per-CASE-expression coverage data tracking which WHEN branches were hit during execution */
public class CaseCoverageData extends NodeData
{
    private final String expressionText;
    private final long[] whenHits;
    private long elseHits;
    private final Location location;

    public CaseCoverageData(int whenClauseCount, String expressionText, Location location)
    {
        this.whenHits = new long[whenClauseCount];
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

    /** Record that a specific WHEN branch was matched for the given row count. */
    public void recordWhenHit(int branchIndex, long count)
    {
        if (branchIndex >= 0
                && branchIndex < whenHits.length
                && count > 0)
        {
            whenHits[branchIndex] += count;
        }
    }

    /** Record that the ELSE branch was matched for the given row count. */
    public void recordElseHit(long count)
    {
        if (count > 0)
        {
            elseHits += count;
        }
    }

    public long[] getWhenHits()
    {
        return whenHits;
    }

    public long getElseHits()
    {
        return elseHits;
    }
}
