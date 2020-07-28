package org.kuse.payloadbuilder.core.catalog.builtin;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.builtin.AMatchFunction.MatchType;

/** Built in catalog with functions etc. */
public class BuiltinCatalog
{
    public static final String NAME = "BUILTIN";
    
    /** Register built in catalog */
    public static Catalog get()
    {
        Catalog catalog = new BuiltInCatalog();
        
        // Scalar functions
        catalog.registerFunction(new ConcatFunction(catalog));
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new SumFunction(catalog));
        catalog.registerFunction(new NowFunction(catalog));
        catalog.registerFunction(new FlatMapFunction(catalog));
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.ANY)
        {
        });
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.ALL)
        {
        });
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.NONE)
        {
        });
        catalog.registerFunction(new RandomInt(catalog));
        catalog.registerFunction(new CountFunction(catalog));
        catalog.registerFunction(new IsNullFunction(catalog));
        catalog.registerFunction(new CoalesceFunction(catalog));
        catalog.registerFunction(new JsonValueFunction(catalog));
        catalog.registerFunction(new CastFunction(catalog, "cast"));
        catalog.registerFunction(new CastFunction(catalog, "convert"));
        
        // Table functions
        catalog.registerFunction(new Range(catalog));
        catalog.registerFunction(new MapToRowFunction(catalog));
        
        return catalog;
    }
    
    private static class BuiltInCatalog extends Catalog
    {
        BuiltInCatalog()
        {
            super(NAME);
        }
    }
}
