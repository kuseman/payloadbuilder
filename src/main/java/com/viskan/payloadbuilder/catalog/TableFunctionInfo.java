package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.OperatorContext;

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

    /** Open iterator for this function 
     * @param context Context
     * @param tableAlias Table alias used for this function
     * @param arguments Arguments to function
     **/
    public abstract Iterator<Row> open(OperatorContext context, TableAlias tableAlias, List<Object> arguments);
}
