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
        
        /** Register funtions */
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new SumFunction(catalog));
        catalog.registerFunction(new NowFunction(catalog));
    }
}
