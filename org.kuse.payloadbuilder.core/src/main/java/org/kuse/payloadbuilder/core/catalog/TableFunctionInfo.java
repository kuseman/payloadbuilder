package org.kuse.payloadbuilder.core.catalog;

import static java.util.Collections.singleton;

import java.util.List;
import java.util.Set;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Definition of a table valued function These functions are applied row by row.
 **/
//CSOFF
public abstract class TableFunctionInfo extends FunctionInfo
//CSON
{
    public TableFunctionInfo(Catalog catalog, String name)
    {
        super(catalog, name, Type.TABLE);
    }

    /**
     * Resolves resulting aliases for this function
     *
     * <pre>
     *
     * Example:
     * Range function
     * <b>range(1, 10)</b>
     * Resulting alias will the table alias for the function itself
    
     * Example:
     * Table valued function that opens an alias for row traversal in a sub query expression
     * <b>open(aa)</b>
     * Resulting alias will the argument result ie. [aa]
     * </pre>
     *
     * @param alias The table alias belonging to the table function
     * @param parentAliases Parent aliases in context to this function
     * @param argumentAliases Resulting aliases for earch function argument
     **/
    public Set<TableAlias> resolveAlias(TableAlias alias, Set<TableAlias> parentAliases, List<Set<TableAlias>> argumentAliases)
    {
        return singleton(alias);
    }

    /**
     * Open iterator for this function
     *
     * <pre>
     * NOTE! In main loop of operator add check of {@link QuerySession#abortQuery()} to not hang a
     * thread in execution state.
     * </pre>
     *
     * @param context Context
     * @param tableAlias Table alias used for this function
     * @param arguments Arguments to function
     **/
    public abstract RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments);
}
