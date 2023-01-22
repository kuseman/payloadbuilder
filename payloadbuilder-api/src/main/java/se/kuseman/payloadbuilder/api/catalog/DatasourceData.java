package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Data used when a catalog is asked for creation of a data source */
public class DatasourceData
{
    private final int nodeId;
    private final List<IPredicate> predicates;
    private final List<? extends ISortItem> sortItems;
    private final List<String> projection;

    public DatasourceData(int nodeId, List<IPredicate> predicates, List<? extends ISortItem> sortItems, List<String> projection)
    {
        this.nodeId = nodeId;
        this.predicates = requireNonNull(predicates, "predicates");
        this.sortItems = requireNonNull(sortItems, "sortItems");
        this.projection = requireNonNull(projection, "projection");
    }

    /**
     * Return the node id for this datasource. The operator ID that this data source belongs to. Can be used in {@link IStatementContext#getNodeData(Integer)} to store custom data.
     */
    public int getNodeId()
    {
        return nodeId;
    }

    /** Return predicates if any to push down to data source if supported by catalog */
    public List<IPredicate> getPredicates()
    {
        return predicates;
    }

    /** Return sort items if any to push down to data soource if supported by catalog. NOTE! All or no sort items must be consumed */
    public List<? extends ISortItem> getSortItems()
    {
        return sortItems;
    }

    /**
     * Return the projected columns needed for this data source. This is a optimization hint that can be used for catalogs that supports fetching of specific columns only like RDMBS:es. If empty then
     * all column should be returned
     */
    public List<String> getProjection()
    {
        return projection;
    }

    /**
     * Convenience method for extracting a equals predicate for specific column.
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
     * 
     * NOTE! Mutates the {@link #predicates} list if column was found.
     * </pre>
     */
    public IExpression extractEqualsPredicate(String column)
    {
        if (predicates.isEmpty())
        {
            return null;
        }

        Iterator<IPredicate> it = predicates.iterator();
        while (it.hasNext())
        {
            IPredicate pair = it.next();
            QualifiedName qname = pair.getQualifiedColumn();
            if (qname == null
                    || pair.getType() != IPredicate.Type.COMPARISION
                    || pair.getComparisonType() != IComparisonExpression.Type.EQUAL)
            {
                continue;
            }
            if (column.equalsIgnoreCase(qname.toDotDelimited()))
            {
                it.remove();
                return pair.getComparisonExpression();
            }
        }
        return null;
    }
}
