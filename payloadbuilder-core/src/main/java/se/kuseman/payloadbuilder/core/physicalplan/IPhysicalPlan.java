package se.kuseman.payloadbuilder.core.physicalplan;

import static java.lang.System.lineSeparator;
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Definition of a physical plan */
public interface IPhysicalPlan extends DescribableNode
{
    /** Return the plans unique node id */
    @Override
    int getNodeId();

    /**
     * Return the output schema for this plan. If this plan is made of static schema components a schema will be returned else an empty schema is returned and the actual schema will be returned via
     * {@link TupleVector#getSchema()} runtime.
     */
    Schema getSchema();

    /**
     * Execute this plan and start generating a tuple vector stream. NOTE! It's important to perform all expression evaluations in either this method or in {@link TupleIterator#next()} and not delay
     * by returning a lazy {@link TupleVector} since there can be expressions depending on outer loop values in {@link IExecutionContext}
     */
    TupleIterator execute(IExecutionContext context);

    /** Return this plans children */
    List<IPhysicalPlan> getChildren();

    /** Returns true if this plan has any writable output else false */
    default boolean hasWritableOutput()
    {
        return true;
    }

    /** Print plan indented */
    default String print(int indent)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(repeat("  ", indent))
                .append(toString())
                .append(lineSeparator());

        for (IPhysicalPlan child : getChildren())
        {
            sb.append(child.print(indent + 1));
        }

        return sb.toString();
    }
}
