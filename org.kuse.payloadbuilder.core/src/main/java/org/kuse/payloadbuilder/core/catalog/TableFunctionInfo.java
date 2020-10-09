package org.kuse.payloadbuilder.core.catalog;

import java.util.List;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
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

    /** Returns columns for this function or null if this function's columns is dynamic. */
    public String[] getColumns()
    {
        return null;
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
