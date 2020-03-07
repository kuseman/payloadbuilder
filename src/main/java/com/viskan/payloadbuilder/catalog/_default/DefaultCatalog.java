package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;

/** Default catalog with built in functions etc. */
public class DefaultCatalog
{
    public static final String NAME = "DEFAULT";
    
    /** Register default schema */
    public static void register(CatalogRegistry catalogRegistry)
    {
        Catalog catalog = new Catalog(NAME);
        catalogRegistry.registerCatalog(catalog);
        
        // Scalar funtions
        catalog.registerFunction(new ConcatFunction(catalog));
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new SumFunction(catalog));
        catalog.registerFunction(new NowFunction(catalog));
        catalog.registerFunction(new FlatMapFunction(catalog));
        
        // Table functions
        catalog.registerFunction(new Range(catalog));
        catalog.registerFunction(new MapToRowFunction(catalog));
    }
}
