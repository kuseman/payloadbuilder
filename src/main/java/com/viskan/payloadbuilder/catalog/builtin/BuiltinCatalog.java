package com.viskan.payloadbuilder.catalog.builtin;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;

/** Built in catalog with functions etc. */
public class BuiltinCatalog
{
    public static final String NAME = "BUILTIN";
    
    /** Register built in catalog */
    public static void register(CatalogRegistry catalogRegistry)
    {
        Catalog catalog = new BuiltInCatalog();
        catalogRegistry.registerCatalog(catalog);
        
        // Scalar functions
        catalog.registerFunction(new ConcatFunction(catalog));
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new SumFunction(catalog));
        catalog.registerFunction(new NowFunction(catalog));
        catalog.registerFunction(new FlatMapFunction(catalog));
        catalog.registerFunction(new RandomInt(catalog));
        
        // Table functions
        catalog.registerFunction(new Range(catalog));
        catalog.registerFunction(new MapToRowFunction(catalog));
    }
    
    private static class BuiltInCatalog extends Catalog
    {
        BuiltInCatalog()
        {
            super(NAME);
        }
    }
}
