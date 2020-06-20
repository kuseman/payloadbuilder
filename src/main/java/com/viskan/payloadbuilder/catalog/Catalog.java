package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Catalog. Top schema type, which defines the hooking points
 * for retrieving data, functions etc. */
public abstract class Catalog
{
    /** Name of the catalog */
    private final String name;
    /** Functions */
    private final Map<String, FunctionInfo> functionByName = new HashMap<>();
    
    public Catalog(String name)
    {
        this.name = requireNonNull(name, "name");
    }
    
    public String getName()
    {
        return name;
    }
    
    /** Get indices for provided table
     * @param table Table to retrieve index for
     **/
    public List<Index> getIndices(QualifiedName table)
    {
        return emptyList();
    }
    
    /** Get operator for provided alias
     * @param nodeId Unique id for operator node
     * @param alias Alias to retrieve operator for
     */ 
    public Operator getScanOperator(int nodeId, TableAlias alias)
    {
        throw new IllegalArgumentException("Catalog " + name + " doesn't support scan operators.");
    }
    
    /** Get operator for provided alias
     * @param nodeId Unique id for operator node
     * @param alias Alias to retrieve operator for
     * @param index Index to use
     */ 
    public Operator getIndexOperator(int nodeId, TableAlias alias, Index index)
    {
        throw new IllegalArgumentException("Catalog " + name + "  doesn't support index operators.");
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
