package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public Schema getSchema(List<IExpression> arguments, List<Option> options)
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
     * @see #execute(IExecutionContext, String, Optional, List, IDatasourceOptions, int)
     * @param nodeId The id of the functions node id the query plan tree. Can be used to get hold of node data from {@link IStatementContext} to store statistics during execution etc.
     */
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options, int nodeId)
    {
        return execute(context, catalogAlias, schema, arguments, options);
    }

    /**
     * Execute table function.
     *
     * @param context Execution context
     * @param catalogAlias The query specific catalog alias used in this invocation
     * @param schema Planned schema for this table function. If this has a value then the schema should be used when returning {@link TupleVector}' from {@link TupleIterator}. Else value will be
     * {@link Optional#empty()} and the actual runtime schema should be return for vectors.
     * @param arguments Function arguments
     * @param options Options for this table source such as batch size etc.
     */
    public abstract TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options);

    /**
     * Returns a map with describe properties that is used during describe/analyze statements
     */
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return emptyMap();
    }

    /** Exception that can be thrown during schema resolving for TVS to properly trigg compile exception. */
    public static class SchemaResolveException extends RuntimeException
    {
        public SchemaResolveException(String message)
        {
            super(message);
        }
    }
}
