package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.session.IQuerySession;

/**
 * Catalog. Defines the hooking points for retrieving data, functions etc.
 */
// CSOFF
public abstract class Catalog
// CSON
{
    protected static final String SYS_TABLES = "tables";
    protected static final String SYS_TABLES_NAME = "name";

    protected static final String SYS_COLUMNS = "columns";
    protected static final String SYS_COLUMNS_NAME = "name";
    protected static final String SYS_COLUMNS_TABLE = "table";

    protected static final String SYS_FUNCTIONS = "functions";
    protected static final String SYS_FUNCTIONS_NAME = "name";
    protected static final String SYS_FUNCTIONS_DESCRIPTION = "description";
    protected static final String SYS_FUNCTIONS_TYPE = "type";

    protected static final String SYS_INDICES = "indices";
    protected static final String SYS_INDICES_TABLE = "table";
    protected static final String SYS_INDICES_COLUMNS = "columns";

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
     * @return List of indices for provided table
     **/
    public List<Index> getIndices(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        return emptyList();
    }

    /** Return registered functions for this catalog */
    public Collection<FunctionInfo> getFunctions()
    {
        return functionByName.values();
    }

    /**
     * Get operator for provided data
     *
     * <pre>
     * NOTE! In main loop of operator add check of {@link IQuerySession#abortQuery()} to not hang a
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
     * NOTE! In main loop of operator add check of {@link IQuerySession#abortQuery()} to not hang a
     * thread in execution state.
     * </pre>
     *
     * @param indexPredicate Predicate to use
     */
    public Operator getIndexOperator(OperatorData data, IIndexPredicate indexPredicate)
    {
        throw new IllegalArgumentException("Catalog " + data.catalogAlias + " (" + name + ") doesn't support index operators.");
    }

    /**
     * <pre>
     * Get system operator for provided data.
     * This method should return a system operator for various system tables like:
     *  - tables
     *     - Return tables in catalog
     *     - Preferable to return at least one column 'name'
     *  - columns
     *     - Return columns in catalog
     *     - Preferable to return at least two columns 'table', 'name'
     *  - indices
     *     - Return indices in catalog
     *     - Preferable to return at least two columns 'table', 'columns'
     *  - functions
     *     - Return functions in catalog
     *     - Preferable to return at least two columns 'name', 'description'
     *
     * NOTE! It's optional to implement this method, but it's a good way to expose
     *       things that the catalog supports
     * NOTE! It's perfectly fine to support other system tables than listed above
     * </pre>
     */
    public Operator getSystemOperator(OperatorData data)
    {
        throw new IllegalArgumentException("Catalog " + data.catalogAlias + " (" + name + ") doesn't support system operators.");
    }

    /** Register function */
    public void registerFunction(FunctionInfo functionInfo)
    {
        requireNonNull(functionInfo);
        functionByName.put(functionInfo.getName()
                .toLowerCase(), functionInfo);
    }

    /** Get function info by name */
    public FunctionInfo getFunction(String name)
    {
        return functionByName.get(requireNonNull(name).toLowerCase());
    }

    /**
     * Helper method that returns an operator with registered functions. Can be used by catalog implementations in {@link #getSystemOperator(OperatorData)}
     **/
    protected Operator getFunctionsOperator(int nodeId, TableAlias tableAlias)
    {
        return systemOperator(nodeId, SYS_FUNCTIONS, ctx ->
        {
            String[] columns = new String[] { SYS_FUNCTIONS_NAME, SYS_FUNCTIONS_TYPE, SYS_FUNCTIONS_DESCRIPTION };
            return TupleIterator.wrap(functionByName.values()
                    .stream()
                    .map(f -> (Tuple) Row.of(tableAlias, columns, new Object[] {
                            f.getName(), f.getType()
                                    .toString(),
                            f.getDescription() }))
                    .iterator());
        });
    }

    /** Class containing data used by Catalog implementations to create operators */
    public static class OperatorData
    {
        private final IQuerySession session;
        private final int nodeId;
        private final String catalogAlias;
        private final TableAlias tableAlias;
        private final List<IAnalyzePair> predicatePairs;
        private final List<ISortItem> sortItems;

        public OperatorData(IQuerySession session, int nodeId, String catalogAlias, TableAlias tableAlias, List<IAnalyzePair> predicatePairs, List<ISortItem> sortItems)
        {
            this.session = session;
            this.nodeId = nodeId;
            this.catalogAlias = catalogAlias;
            this.tableAlias = tableAlias;
            this.predicatePairs = requireNonNull(predicatePairs, "predicatePairs");
            this.sortItems = requireNonNull(sortItems, "sortItems");
        }

        /** Current query session */
        public IQuerySession getSession()
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
         * NOTE! If {@link TableAlias#getTableMeta()} is specified then {@link Tuple Tuples} should follow.
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
        public List<IAnalyzePair> getPredicatePairs()
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
        public List<ISortItem> getSortItems()
        {
            return sortItems;
        }

        /**
         * Extract value supplier for right hand side of predicate for provided column
         *
         * <pre>
         *   Ie.
         *
         *   select *
         *   from table t
         *   where 'val' = t.col
         *
         * Calling this method with value 'col' would return in
         * the literal string expression 'val'
         * </pre>
         */
        public IExpression extractPredicate(String column)
        {
            if (predicatePairs.isEmpty())
            {
                return null;
            }

            Iterator<IAnalyzePair> it = predicatePairs.iterator();
            while (it.hasNext())
            {
                IAnalyzePair pair = it.next();
                if (column.equalsIgnoreCase(pair.getColumn(tableAlias.getAlias())))
                {
                    it.remove();
                    return pair.getComparisonExpression(tableAlias.getAlias());
                }
            }
            return null;
        }
    }

    /** Helper method to create a system operator */
    protected Operator systemOperator(int nodeId, String name, Function<IExecutionContext, TupleIterator> iterator)
    {
        return new Operator()
        {
            @Override
            public int getNodeId()
            {
                return nodeId;
            }

            @Override
            public String getName()
            {
                return "System (" + name + ")";
            }

            @Override
            public TupleIterator open(IExecutionContext context)
            {
                return iterator.apply(context);
            }
        };
    }
}
