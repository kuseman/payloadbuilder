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
//    private final Catalog catalog;
//    private final String name;

    public TableFunctionInfo(Catalog catalog, String name, Type type)
    {
        super(catalog, name, type);
//        this.catalog = requireNonNull(catalog, "catalog");
//        this.name = requireNonNull(name, "name");
    }
    
//    public String getName()
//    {
//        return name;
//    }

    /** Open iterator for this function 
     * @param context Context
     * @param tableAlias Table alias used for this function
     * @param arguments Arguments to function
     **/
    public abstract Iterator<Row> open(OperatorContext context, TableAlias tableAlias, List<Object> arguments);
    
//    @Override
//    public String toString()
//    {
//        return (DefaultCatalog.NAME.equals(catalog.getName()) ? "" : (catalog.getName() + ".")) + name;
//    }
}
