package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Data used when a catalog is asked for creation of a data source */
public class DatasourceData
{
    private final int nodeId;
    private final List<IPredicate> predicates;
    private final List<? extends ISortItem> sortItems;
    private final Projection projection;
    private final List<Option> options;

    public DatasourceData(int nodeId, List<IPredicate> predicates, List<? extends ISortItem> sortItems, Projection projection, List<Option> options)
    {
        this.nodeId = nodeId;
        this.predicates = requireNonNull(predicates, "predicates");
        this.sortItems = requireNonNull(sortItems, "sortItems");
        this.projection = requireNonNull(projection, "projection");
        this.options = requireNonNull(options, "options");
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
     * Return the wanted projection for this data source. This is a optimization hint that can be used for catalogs that supports fetching of specific columns only like RDMBS:es.
     *
     * <pre>
     * NOTE! If Catalog implementation has a specified {@link Schema} returned from {@link Catalog#getScanDataSource} or {@link Catalog#getSeekDataSource} that schema must be respected
     * when applying a projection.
     *
     * Ie.
     *
     *  Schema for 'cusomter'
     *   - id    INT
     *   - name  STRING
     *   - group STRING
     *
     * And projection wanted is [id, group] then the returned {@link TupleVector} must look like the original schema:
     *
     * - id: [1,2,3]
     * - name: [null, null, null]
     * - group: ["group1", "group2", "group3" ]
     *
     * </pre>
     */
    public Projection getProjection()
    {
        return projection;
    }

    /** Return defined options for this data source */
    public List<Option> getOptions()
    {
        return options;
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
    public IExpression extractEqualsPredicate(QualifiedName column)
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
            if (qname.equals(column))
            {
                it.remove();
                return pair.getComparisonExpression();
            }
        }
        return null;
    }

    /** A projection that is wanted from the data source. */
    public record Projection(ProjectionType type, List<String> columns)
    {

        public static final Projection NONE = new Projection(ProjectionType.NONE, emptyList());
        public static final Projection ALL = new Projection(ProjectionType.ALL, emptyList());

        /** Construct a projection. */
        public Projection
        {
            columns = unmodifiableList(requireNonNull(columns));
            if ((type == ProjectionType.ALL
                    || type == ProjectionType.NONE)
                    && !columns.isEmpty())
            {
                throw new IllegalArgumentException("Columns list must be empty when type is ALL or NONE");
            }
            else if (type == ProjectionType.COLUMNS
                    && columns.isEmpty())
            {
                throw new IllegalArgumentException("Columns list must be NOT empty when type is COLUMNS");
            }
        }

        /** Constructs a projection with provided columns. */
        public static Projection columns(List<String> columns)
        {
            return new Projection(ProjectionType.COLUMNS, columns);
        }
    }

    /** Definition of different projection types that sent to Catalog implementations when a specific projection is asked for. */
    public enum ProjectionType
    {
        /**
         * All available columns should be returned from datasource. Typically the case when an asterisk (*) select is targeting the data source.
         */
        ALL,
        /** No projection is wanted. This is a special case when the datasource is present in the operator tree but no columns is asked from it. */
        NONE,
        /** A set of columns is wanted. */
        COLUMNS;
    }
}
