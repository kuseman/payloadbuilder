package se.kuseman.payloadbuilder.core.codegen;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.operator.Projection;
import se.kuseman.payloadbuilder.core.operator.RootProjection;

/** Base class for generated projections */
// CSOFF
public abstract class BaseProjection extends BaseGeneratedClass implements Projection, DescribableNode
// CSON
{
    private RootProjection projection;

    void setProjection(RootProjection projection)
    {
        this.projection = projection;
    }

    public RootProjection getProjection()
    {
        return projection;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return projection.getChildNodes();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return projection.getDescribeProperties(context);
    }

    @Override
    public String getName()
    {
        return "Gen: " + projection.getName();
    }
}
