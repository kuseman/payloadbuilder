package se.kuseman.payloadbuilder.api.catalog;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Definition of a table valued function. These functions are applied row by row.
 **/
// CSOFF
public abstract class TableFunctionInfo extends FunctionInfo
// CSON
{
    public TableFunctionInfo(Catalog catalog, String name)
    {
        super(catalog, name, FunctionType.TABLE);
    }

    /**
     * Convenience method for evaluating an expression where there is no tuple vector input. For example a table function that is always a leaf. This method uses a constant scan {@link TupleVector} to
     * always return one row.
     */
    protected ValueVector eval(IExecutionContext context, IExpression expression)
    {
        return expression.eval(TupleVector.CONSTANT, context);
    }

    // /**
    // * Resolves resulting aliases for this function
    // *
    // * <pre>
    // *
    // * Example:
    // * Range function
    // * <b>range(1, 10)</b>
    // * Resulting alias will the table alias for the function itself
    //
    // * Example:
    // * Table valued function that opens an alias for row traversal in a sub query expression
    // * <b>open_rows(aa)</b>
    // * Resulting alias will the argument result ie. [aa]
    // * </pre>
    // *
    // * @param alias The table alias belonging to the table function
    // * @param parentAliases Parent aliases in context to this function
    // * @param argumentAliases Resulting aliases for earch function argument
    // **/
    // public Set<TableAlias> resolveAlias(TableAlias alias, Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    // {
    // return singleton(alias);
    // }
    //
    // /** Return table meta for this function */
    // public TableMeta getTableMeta()
    // {
    // return null;
    // }

    // /**
    // * Open iterator for this function
    // *
    // * <pre>
    // * NOTE! In main loop of operator add check of {@link IQuerySession#abortQuery()} to not hang a
    // * thread in execution state.
    // * </pre>
    // *
    // * @param context Context
    // * @param tableAlias Table alias used for this function
    // * @param arguments Arguments to function
    // **/
    // public abstract TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments);

    /* New methods plb 1.x */

    /**
     * Return the schema for this function.
     */
    public Schema getSchema(List<? extends IExpression> arguments)
    {
        return Schema.EMPTY;
    }

    /** Execute table function. */
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        return TupleIterator.EMPTY;
    }
}
