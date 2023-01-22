package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.common.Option;

/** Implementation of {@link IDatasourceOptions} */
public class DatasourceOptions implements IDatasourceOptions
{
    private static final String BATCH_SIZE = "batch_size";
    private static final int DEFAULT_BATCH_SIZE = 500;
    private final List<Option> options;

    public DatasourceOptions(List<Option> options)
    {
        this.options = requireNonNull(options, "options");
    }

    @Override
    public int getBatchSize(IExecutionContext context)
    {
        ValueVector v = getOption(BATCH_SIZE, context);
        if (v != null
                && !v.isNull(0))
        {
            return v.getInt(0);
        }

        // TOOD: fetch default settings from system variables
        return DEFAULT_BATCH_SIZE;
    }

    @Override
    public ValueVector getOption(String name, IExecutionContext context)
    {
        for (Option option : options)
        {
            if (option.getOption()
                    .toDotDelimited()
                    .equalsIgnoreCase(name))
            {
                return option.getValueExpression()
                        .eval(context);
            }
        }
        return null;
    }

}
