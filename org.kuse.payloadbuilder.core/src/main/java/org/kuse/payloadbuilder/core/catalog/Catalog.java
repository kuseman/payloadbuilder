package org.kuse.payloadbuilder.core.catalog;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Option;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.SortItem;

/**
 * Catalog. Defines the hooking points for retrieving data, functions etc.
 */
//CSOFF
public abstract class Catalog
//CSON
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
     * @param table Table to retrieve indices for
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
     * to allow "SHOW tables" statement to list tables in
     * current catalog.
     * </pre>
     *
     * @param session Current query session
     * @param catalogAlias Alias used for this catalog in the query
     */
    public List<String> getTables(
            QuerySession session,
            String catalogAlias)
    {
        return emptyList();
    }

    /**
     * Get operator for provided data
     *
     * <pre>
     * NOTE! In main loop of operator add check of {@link QuerySession#abortQuery()} to not hang a
     * thread in execution state.
     * </pre>
     */
    public Operator getScanOperator(OperatorData data)
    {
        throw new IllegalArgumentException("Catalog " + data.catalogAlias + " (" + name + ") doesn't support scan operators.");
    }

    /**
     * Get operator for provided alias
     *
     * <pre>
     * NOTE! In main loop of operator add check of {@link QuerySession#abortQuery()} to not hang a
     * thread in execution state.
     * </pre>
     *
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

    /** Class containing data used by Catalog implementations to create operators */
    public static class OperatorData
    {
        private final QuerySession session;
        private final int nodeId;
        private final String catalogAlias;
        private final TableAlias tableAlias;
        private final List<AnalyzePair> predicatePairs;
        private final List<SortItem> sortItems;
        private final List<Option> tableOptions;

        public OperatorData(
                QuerySession session,
                int nodeId,
                String catalogAlias,
                TableAlias tableAlias,
                List<AnalyzePair> predicatePairs,
                List<SortItem> sortItems,
                List<Option> tableOptions)
        {
            this.session = session;
            this.nodeId = nodeId;
            this.catalogAlias = catalogAlias;
            this.tableAlias = tableAlias;
            this.predicatePairs = requireNonNull(predicatePairs, "predicatePairs");
            this.sortItems = requireNonNull(sortItems, "sortItems");
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
         * NOTE! Properties {@link TableAlias#isAsteriskColumns()} and {@link TableAlias#getColumns()}
         * is runtime values and should not be considered when creating an operator but rather executing
         * the operator
         * </pre>
         */
        public TableAlias getTableAlias()
        {
            return tableAlias;
        }

        /**
         * <pre>
         * Predicate pairs for a table if any.
         * Is sent to catalog by framework to let catalogs that supports
         * predicate extract supported predicate pairs and return left over ones.
         *
         * Example
         * table:   article
         * pairs:   active_flg = true
         *          internet_flg = true
         *
         * Catalog is able to filter active_flg on it's own
         * but not internet_flg
         * then pair <b>active_flg = true</b> is consumed and <b>internet_flg = true</b>
         * is returned to framework for filter.
         * </pre>
         **/
        public List<AnalyzePair> getPredicatePairs()
        {
            return predicatePairs;
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
        public List<Option> getTableOptions()
        {
            return tableOptions;
        }
    }
}
