package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import java.util.Iterator;
import java.util.List;

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
     * @param arguments Arguments to function
     **/
    public abstract Iterator<Row> open(ExecutionContext context, TableAlias tableAlias, List<Object> arguments);
}
