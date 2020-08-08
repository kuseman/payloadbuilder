package org.kuse.payloadbuilder.core.catalog;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.SortItem;
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
     *
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
     * Return registered functions in this Catalog 
     **/
    public Collection<FunctionInfo> getFunctions()
    {
        return functionByName.values();
    }
    
    /** 
     * <pre>
     * Get tables for current session.
     * Implementations can choose to implement this method
     * to allow "SHOW tables" for list current tables
     * </pre>
     * @param session Current query session
     * @param catalogAlias Alias used for this catalog in the query
     * */
    public List<String> getTables(
            QuerySession session,
            String catalogAlias)
    {
        return emptyList();
    }

    /**
     * Get operator for provided alias
     */
    public Operator getScanOperator(OperatorData data)
    {
        throw new IllegalArgumentException("Catalog " + data.catalogAlias + " (" + name + ") doesn't support scan operators.");
    }

    /**
     * Get operator for provided alias
     * @param index Index to use
     */
    public Operator getIndexOperator(OperatorData data, Index index)
    {
        throw new IllegalArgumentException("Catalog " + data.catalogAlias + " (" + name + ") doesn't support index operators.");
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

    /**
     * Predicate for a table if any. Is sent to catalog by framework to let catalogs that supports predicate analyze and extract supported predicates
     * and return left over ones.
     *
     * <pre>
     *
     * Example
     * table:   article
     * pairs:   active_flg = true
     *          internet_flg = true
     *
     * Catalog is able to filter active_flg on it's own
     * but not internet_flg
     * then pair <b>active_flg = true</b> is consumed and <b>internet_flg</b>
     * is returned to framework for filter.
     * </pre>
     **/
    public static class TablePredicate
    {
        public static TablePredicate EMPTY = new TablePredicate(emptyList());
        private final List<AnalyzePair> pairs;

        public TablePredicate(List<AnalyzePair> pairs)
        {
            this.pairs = ObjectUtils.defaultIfNull(pairs, emptyList());
        }
        
        public List<AnalyzePair> getPairs()
        {
            return pairs;
        }
    }

    /** Class containing data used by Catalog implementations to create operators */
    public static class OperatorData
    {
        private final QuerySession session;
        private final int nodeId;
        private final String catalogAlias;
        private final TableAlias tableAlias;
        private final TablePredicate predicate;
        private final List<SortItem> sortItems;
        private final List<TableOption> tableOptions;

        public OperatorData(
                QuerySession session,
                int nodeId,
                String catalogAlias,
                TableAlias tableAlias,
                TablePredicate predicate,
                List<SortItem> sortItems,
                List<TableOption> tableOptions)
        {
            this.session = session;
            this.nodeId = nodeId;
            this.catalogAlias = catalogAlias;
            this.tableAlias = tableAlias;
            this.predicate = predicate;
            this.sortItems = sortItems;
            this.tableOptions = tableOptions;
        }

        /** Current query session */
        public QuerySession getSession()
        {
            return session;
        }

        /** Unique id for operator node */
        public int getNodeId()
        {
            return nodeId;
        }

        /** Alias used for this catalog in the query */
        public String getCatalogAlias()
        {
            return catalogAlias;
        }

        /**
         * <pre>
         * Table alias to retrieve operator for.
         * If {@link TableAlias#isAsteriskColumns()} is set to true then the operator
         *  <b>MUST</b> set all available columns on alias using {@link TableAlias#setColumns(String[])} 
         * </pre>
         */ 
        public TableAlias getTableAlias()
        {
            return tableAlias;
        }
        
        /**
         * <pre>
         * Predicate (if any) for this table.
         * Should be consumed by implementation if applicable, this to push down
         * predicates as much as possible. 
         * </pre>
         * */
        public TablePredicate getPredicate()
        {
            return predicate;
        }

        /**
         * <pre>
         * Sort items (if any) for this table.
         * Should be consumed by implementation  if applicable or leaved if not supported,
         * this to push down ordering as much as possible
         * </pre>
         */
        public List<SortItem> getSortItems()
        {
            return sortItems;
        }

        /** Provided options for this table. */
        public List<TableOption> getTableOptions()
        {
            return tableOptions;
        }
    }
}
