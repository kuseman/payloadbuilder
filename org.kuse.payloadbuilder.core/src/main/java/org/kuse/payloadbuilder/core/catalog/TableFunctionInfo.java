package org.kuse.payloadbuilder.core.catalog;

import java.util.Iterator;
import java.util.List;

import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Definition of a table valued function
 * These functions are applied row by row.
 **/
public abstract class TableFunctionInfo extends FunctionInfo
{
    public TableFunctionInfo(Catalog catalog, String name, Type type)
    {
        super(catalog, name, type);
    }

    /** Returns columns for this function or null if this function's columns is dynamic. */
    public String[] getColumns()
    {
        return null;
    }
    
    /** Open iterator for this function 
     * @param context Context
     * @param tableAlias Table alias used for this function
     * NOTE! If columns are null the function must set ALL available columns for the function
     * @param arguments Arguments to function
     **/
    public abstract Iterator<Row> open(ExecutionContext context, TableAlias tableAlias, List<Object> arguments);
}
