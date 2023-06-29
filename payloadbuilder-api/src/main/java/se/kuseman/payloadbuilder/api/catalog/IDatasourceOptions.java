package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Definition of options provided to data sources. These are options within a WITH statement in table sources
 */
public interface IDatasourceOptions
{
    /** Return the vector batch size. This is simply a convenience method for {@link #getOption(String, IExecutionContext)} with option name batch_size */
    int getBatchSize(IExecutionContext context);

    /** Return value for provided option name. If no option found with provided name null is returned */
    ValueVector getOption(String name, IExecutionContext context);
}
