package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Definition of a table valued function. These functions are applied row by row.
 **/
// CSOFF
public abstract class TableFunctionInfo extends FunctionInfo
// CSON
{
    public TableFunctionInfo(String name)
    {
        super(name, FunctionType.TABLE);
    }

    /**
     * Return the schema for this function. NOTE! If schema cannot be resolved throw a {@link SchemaResolveException}
     */
    public Schema getSchema(IExecutionContext context, String catalogAlias, List<IExpression> arguments, List<Option> options)
    {
        return getSchema(arguments);
    }

    /**
     * Return the schema for this function. NOTE! If schema cannot be resolved throw a {@link SchemaResolveException}
     */
    public Schema getSchema(List<IExpression> arguments)
    {
        return Schema.EMPTY;
    }

    /**
     * Execute table function.
     *
     * <pre>
     * NOTE! If {{@link #getSchema(List,List)} returned a NON empty schema the {@link TupleVector}'s return from this {@link TupleIterator} MUST
     * have the same set of columns regarding type and name.
     * </pre>
     *
     * @param context Execution context
     * @param catalogAlias The query specific catalog alias used in this invocation
     * @param arguments Function arguments
     * @param functionData Various data connected to the function.
     */
    public abstract TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData functionData);

    /**
     * Returns a map with describe properties that is used during describe/analyze statements
     */
    public Map<String, Object> getDescribeProperties(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData functionData)
    {
        return emptyMap();
    }

    /** Exception that can be thrown during schema resolving for TVF:s to properly trigg compile exception. */
    public static class SchemaResolveException extends RuntimeException
    {
        public SchemaResolveException(String message)
        {
            super(message);
        }
    }

    /** Data used when executing a {@link TableFunctionInfo}. */
    public static class FunctionData
    {
        private final int nodeId;
        private final List<Option> options;

        public FunctionData(int nodeId, List<Option> options)
        {
            this.nodeId = nodeId;
            this.options = requireNonNull(options);
        }

        /**
         * Return the node id that the function belongs to. This can be used to access data in {@link IStatementContext} that uniquely belongs this the node.
         *
         * @return the nodeId or -1 if this function call is not connected to any node.
         */
        public int getNodeId()
        {
            return nodeId;
        }

        /** Get options provided to the function. */
        public List<Option> getOptions()
        {
            return options;
        }
    }
}
