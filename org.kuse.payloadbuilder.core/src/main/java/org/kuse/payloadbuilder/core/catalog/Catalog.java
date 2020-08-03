package org.kuse.payloadbuilder.core.catalog;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.TableOption;

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
     * @param tableAlias Table alias to retrieve operator for. If {@link TableAlias#isAsteriskColumns()()} is set
     * to true then the operator <b>MUST</b> set all available columns on alias using {@link TableAlias#setColumns(String[])}
     * @param predicate Predicate (if any) for this table
     * @param tableOptions Provided options for this table.
     */
    public Operator getScanOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            TablePredicate predicate,
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
     * @param tableAlias Table alias to retrieve operator for. If {@link TableAlias#isAsteriskColumns()()} is set
     * to true then the operator <b>MUST</b> set all available columns on alias using {@link TableAlias#setColumns(String[])}
     * @param index Index to use
     * @param predicate Predicate (if any) for this table
     * @param tableOptions Provided options for this table.
     */
    public Operator getIndexOperator(
            QuerySession session,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index,
            TablePredicate predicate,
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
    
    /** Predicate for a table if any. Is sent to catalog by framework
     * to let catalogs that supports predicate analyze and extract supported
     * predicates and return left over ones.
     * <pre>
     * 
     * Example
     * table:       article
     * predicate:   active_flg && internet_flg
     * 
     * Catalog is able to filter active_flg on it's own
     * but not internet_flg
     * then predicate part <b>active_flg</b> is extracted and <b>internet_flg</b>
     * is returned to framework for filter.
     * </pre>
     **/
    public static class TablePredicate
    {
        public static TablePredicate EMPTY = new TablePredicate(null);
        private Expression predicate;
        public TablePredicate(Expression predicate)
        {
            this.predicate = predicate;
        }
        
        public Expression getPredicate()
        {
            return predicate;
        }
        
        public void setPredicate(Expression predicate)
        {
            this.predicate = predicate;
        }
    }
}
