package se.kuseman.payloadbuilder.api.execution;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorFactory;
import se.kuseman.payloadbuilder.api.expression.IExpressionFactory;

/** Definition of a execution context. */
public interface IExecutionContext
{
    public static final QualifiedName BATCH_SIZE = QualifiedName.of("batch_size");
    public static final int DEFAULT_BATCH_SIZE = 500;

    /** Return the current session */
    IQuerySession getSession();

    /** Return vector builder factory from context */
    IVectorFactory getVectorFactory();

    /** Return the statement context */
    IStatementContext getStatementContext();

    /** Return expression factory */
    IExpressionFactory getExpressionFactory();

    /** Return value of provided variable name. */
    ValueVector getVariableValue(String name);

    /** Return the vector batch size. This is simply a convenience method for {@link #getOption(String, IExecutionContext)} with option name batch_size */
    default int getBatchSize(List<Option> options)
    {
        ValueVector v = getOption(BATCH_SIZE, options);
        if (v != null
                && !v.isNull(0))
        {
            return v.getInt(0);
        }

        // TOOD: fetch default settings from system variables
        return DEFAULT_BATCH_SIZE;
    }

    /** Return value for provided option name. If no option found with provided name null is returned */
    default ValueVector getOption(QualifiedName name, List<Option> options)
    {
        for (Option option : options)
        {
            if (option.getOption()
                    .equalsIgnoreCase(name))
            {
                return option.getValueExpression()
                        .eval(this);
            }
        }
        return null;
    }
}
