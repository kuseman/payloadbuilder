package com.viskan.payloadbuilder.catalog;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

/** Catalog. Top schema type, which defines the hooking points
 * for retrieving data, functions etc. */
public class Catalog
{
    /** Name of the catalog */
    private final String name;
    /** Scalar functions */
    private final Map<String, FunctionInfo> scalarFunctionByName = new HashMap<>();
    // TODO: table's
    //       table functions
    
    public Catalog(String name)
    {
        this.name = requireNonNull(name, "name");
    }
    
    public String getName()
    {
        return name;
    }

    /** Register function */
    public void registerScalarFunction(FunctionInfo functionInfo)
    {
        requireNonNull(functionInfo);
        scalarFunctionByName.put(functionInfo.getName().toLowerCase(), functionInfo);
    }

    /** Get function info by name */
    public FunctionInfo getScalarFunction(String name)
    {
        return scalarFunctionByName.get(requireNonNull(name).toLowerCase());
    }
}
