package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.TableOption;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog. Top schema type, which defines the hooking points for retrieving data, functions etc.
 */
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

    /**
     * Get indices for provided table
     * @param session Current query session
     * @param catalogAlias Alias used for this catalog in the query
     * @param table Table to retrieve index for
     **/
    public List<Index> getIndices(
            QuerySession session,
            String catalogAlias,
            QualifiedName table)
    {
        return emptyList();
    }

    /**
     * Get operator for provided alias
     *
     * @param session Current query session
     * @param nodeId Unique id for operator node
     * @param catalogAlias Alias used for this catalog in the query
     * @param tableAlias Table alias to retrieve operator for
     * @param tableOptions Provided options for this table.
     */
    public Operator getScanOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            List<TableOption> tableOptions)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support scan operators.");
    }

    /**
     * Get operator for provided alias
     *
     * @param session Current query session
     * @param nodeId Unique id for operator node
     * @param catalogAlias Alias used for this catalog in the query
     * @param tableAlias Table alias to retrieve operator for
     * @param index Index to use
     * @param tableOptions Provided options for this table.
     */
    public Operator getIndexOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index,
            List<TableOption> tableOptions)
    {
        throw new IllegalArgumentException("Catalog " + catalogAlias + " (" + name + ") doesn't support index operators.");
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
