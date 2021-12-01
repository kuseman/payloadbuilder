package org.kuse.payloadbuilder.core.codegen;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.DescribableNode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.RootProjection;

/** Base class for generated projections */
//CSOFF
public abstract class BaseProjection extends BaseGeneratedClass implements Projection, DescribableNode
//CSON
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
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return projection.getDescribeProperties(context);
    }

    @Override
    public String getName()
    {
        return "Gen: " + projection.getName();
    }
}
