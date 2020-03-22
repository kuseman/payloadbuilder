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
    /** Functions */
    private final Map<String, FunctionInfo> functionByName = new HashMap<>();
    
    private OperatorFactory operatorFactory;
    
    public Catalog(String name)
    {
        this.name = requireNonNull(name, "name");
    }
    
    public String getName()
    {
        return name;
    }
    
    public OperatorFactory getOperatorFactory()
    {
        return operatorFactory;
    }
    
    public void setOperatorFactory(OperatorFactory operatorFactory)
    {
        this.operatorFactory = operatorFactory;
    }

    /** Register function */
    public void registerFunction(FunctionInfo functionInfo)
    {
        requireNonNull(functionInfo);
        functionByName.put(functionInfo.getName().toLowerCase(), functionInfo);
    }

    /** Get function info by name */
    public FunctionInfo getFunction(String name)
    {
        return functionByName.get(requireNonNull(name).toLowerCase());
    }
}
