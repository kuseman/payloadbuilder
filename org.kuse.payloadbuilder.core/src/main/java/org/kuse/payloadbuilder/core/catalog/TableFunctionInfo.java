package org.kuse.payloadbuilder.core.catalog;

import java.util.List;

import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Definition of a table valued function These functions are applied row by row.
 **/
public abstract class TableFunctionInfo extends FunctionInfo
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
     * @param context Context
     * @param tableAlias Table alias used for this function NOTE If {@link TableAlias#isAsteriskColumns()} is set to true then the operator
     *            <b>MUST</b> set all available columns on alias using {@link TableAlias#setColumns(String[])}
     * @param arguments Arguments to function
     **/
    public abstract RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments);
}
