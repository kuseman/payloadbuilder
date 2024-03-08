package se.kuseman.payloadbuilder.api.catalog;

import java.util.List;
import java.util.Optional;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
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

    /** Exception that can be thrown during schema resolving for TVS to properly trigg compile exception. */
    public static class SchemaResolveException extends RuntimeException
    {
        public SchemaResolveException(String message)
        {
            super(message);
        }
    }
}
