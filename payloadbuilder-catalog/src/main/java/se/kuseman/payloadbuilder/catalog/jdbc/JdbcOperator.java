package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Jdbc Operator */
class JdbcOperator extends AOperator
{
    private final JdbcCatalog catalog;
    private final TableAlias tableAlias;
    private final String catalogAlias;
    private final List<IAnalyzePair> predicatePairs;
    private final List<ISortItem> sortItems;
    private final IIndexPredicate indexPredicate;

    JdbcOperator(JdbcCatalog catalog, int nodeId, String catalogAlias, TableAlias tableAlias, List<IAnalyzePair> predicatePairs, List<ISortItem> sortItems, IIndexPredicate indexPredicate)
    {
        super(nodeId);
        this.catalog = catalog;
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.predicatePairs = predicatePairs;
        this.sortItems = sortItems;
        this.indexPredicate = indexPredicate;
    }

    @Override
    public String getName()
    {
        return (indexPredicate != null ? "index"
                : "scan")
               + " ("
               + tableAlias.getTable()
               + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> result = ofEntries(true, entry(CATALOG, JdbcCatalog.NAME));

        if (!isEmpty(predicatePairs))
        {
            result.put(PREDICATE, predicatePairs.stream()
                    .map(IAnalyzePair::getSqlRepresentation)
                    .collect(joining(" AND ")));
        }
        if (!isEmpty(sortItems))
        {
            result.put("Sort", sortItems.stream()
                    .map(Objects::toString)
                    .collect(joining(",")));
        }

        result.put("Query", buildSql(context));

        return result;
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        return getIterator(catalog, context, catalogAlias, tableAlias, buildSql(context), null);
    }

    // CSOFF
    private String buildSql(IExecutionContext context)
    // CSON
    {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(tableAlias.getTableMeta() == null ? "*"
                : tableAlias.getTableMeta()
                        .getColumns()
                        .stream()
                        .map(c -> c.getName())
                        .collect(joining(",")));
        sb.append(" FROM ");
        sb.append(tableAlias.getTable()
                .toString())
                .append(" y ");

        boolean whereAdded = false;
        if (!predicatePairs.isEmpty())
        {
            for (IAnalyzePair pair : predicatePairs)
            {
                if (!whereAdded)
                {
                    sb.append(" WHERE ");
                    whereAdded = true;
                }
                else
                {
                    sb.append(" AND ");
                }
                String alias = tableAlias.getAlias();
                QualifiedName qname = pair.getQualifiedName(alias);
                // CSOFF
                switch (pair.getType())
                // CSON
                {
                    case UNDEFINED:
                        throw new IllegalArgumentException("Illegal predicate type");
                    case COMPARISION:
                        Object value = convertValue(pair.getComparisonExpression(alias)
                                .eval(context));
                        sb.append(qname.toDotDelimited());
                        appendComparisonValue(sb, pair);
                        sb.append(value);
                        break;
                    case IN:

                        IInExpression inExpression = pair.getInExpression(alias);
                        sb.append(qname.toDotDelimited());

                        if (inExpression.isNot())
                        {
                            sb.append(" NOT");
                        }

                        sb.append(" IN (");
                        sb.append(inExpression.getArguments()
                                .stream()
                                .map(e -> e.eval(context))
                                .filter(Objects::nonNull)
                                .map(o -> convertValue(o))
                                .collect(joining(",")));
                        sb.append(")");
                        break;
                    case LIKE:
                        ILikeExpression likeExpression = pair.getLikeExpression(alias);

                        if (likeExpression.isNot())
                        {
                            sb.append(" NOT");
                        }

                        IExpression valueSupplier = likeExpression.getPatternExpression();
                        sb.append(qname.toDotDelimited());
                        sb.append(" LIKE ");
                        sb.append(convertValue(valueSupplier.eval(context)));
                        break;
                    case NULL:
                        sb.append(qname.toDotDelimited());
                        sb.append(" IS NULL");
                        break;
                    case NOT_NULL:
                        sb.append(qname.toDotDelimited());
                        sb.append(" IS NOT NULL");
                        break;
                }
            }
        }

        if (indexPredicate != null)
        {
            /*
             * Build index sql from context values
             *
             * WHERE ( (col1 = 1 and col2 = 2) or (col1 = 2 and col2 = 3) )
             *
             */

            int size = indexPredicate.getIndexColumns()
                    .size();
            sb.append(whereAdded ? " AND ("
                    : " WHERE (");
            Iterator<IOrdinalValues> it = indexPredicate.getOuterValuesIterator(context);
            // No iterator here, that means we have a describe/analyze-call
            // so add a dummy value
            if (it == null)
            {
                IOrdinalValues dummy = new IOrdinalValues()
                {
                    @Override
                    public int size()
                    {
                        return 1;
                    }

                    @Override
                    public Object getValue(int ordinal)
                    {
                        return "<index values>";
                    }
                };

                it = asList(dummy).iterator();
            }

            while (it.hasNext())
            {
                IOrdinalValues values = it.next();
                sb.append("(");
                for (int i = 0; i < size; i++)
                {
                    if (i > 0)
                    {
                        sb.append(" AND ");
                    }
                    sb.append(indexPredicate.getIndexColumns()
                            .get(i))
                            .append("=");
                    Object value = convertValue(values.getValue(i));
                    sb.append(value);
                }
                sb.append(")");
                if (it.hasNext())
                {
                    sb.append(" OR ");
                }
            }

            sb.append(")");
        }

        if (!sortItems.isEmpty())
        {
            boolean orderByAdded = false;
            for (ISortItem item : sortItems)
            {
                if (!orderByAdded)
                {
                    sb.append(" ORDER BY ");
                    orderByAdded = true;
                }
                else
                {
                    sb.append(", ");
                }

                sb.append(item.getExpression()
                        .getQualifiedName()
                        .getLast());
                sb.append(item.getOrder() == Order.ASC ? " ASC"
                        : " DESC");
            }
        }

        return sb.toString();
    }

    private String convertValue(Object value)
    {
        if (value instanceof Boolean)
        {
            return (Boolean) value ? "1"
                    : "0";
        }
        else if (value instanceof String)
        {
            return "'" + String.valueOf(value) + "'";
        }

        return String.valueOf(value);
    }

    private void appendComparisonValue(StringBuilder sb, IAnalyzePair pair)
    {
        switch (pair.getComparisonType())
        {
            case EQUAL:
                sb.append("=");
                break;
            case GREATER_THAN:
                sb.append(">");
                break;
            case GREATER_THAN_EQUAL:
                sb.append(">=");
                break;
            case LESS_THAN:
                sb.append("<");
                break;
            case LESS_THAN_EQUAL:
                sb.append("<=");
                break;
            case NOT_EQUAL:
                sb.append("!=");
                break;
            default:
                break;
        }
    }

    /** Returns a row iterator with provided query and parameters */
    static TupleIterator getIterator(JdbcCatalog catalog, IExecutionContext context, String catalogAlias, TableAlias tableAlias, String query, List<Object> parameters)
    {
        final String database = context.getSession()
                .getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE);

        // CSOFF
        return new TupleIterator()
        // CSON
        {
            private PreparedStatement statement;
            private ResultSet rs;
            private String[] columns;

            @Override
            public Tuple next()
            {
                if (columns == null)
                {
                    populateMeta();
                }

                int length = columns.length;
                Object[] values = new Object[length];
                for (int i = 0; i < length; i++)
                {
                    try
                    {
                        values[i] = rs.getObject(i + 1);
                    }
                    catch (SQLException e)
                    {
                        throw new RuntimeException("Error fetching value from result set", e);
                    }
                }

                return Row.of(tableAlias, columns, values);
            }

            @Override
            public boolean hasNext()
            {
                try
                {
                    if (rs == null)
                    {
                        setNextResultSet();
                    }
                    return rs != null
                            && rs.next();
                }
                catch (SQLException e)
                {
                    throw new RuntimeException("Error advancing result set", e);
                }
            }

            @Override
            public void close()
            {
                if (statement != null)
                {
                    try
                    {
                        statement.getConnection()
                                .close();
                    }
                    catch (SQLException e)
                    {
                        throw new RuntimeException("Error closing result set", e);
                    }
                    statement = null;
                    rs = null;
                    columns = null;
                }
            }

            private void populateMeta()
            {
                try
                {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int count = metaData.getColumnCount();
                    columns = new String[count];

                    for (int i = 0; i < count; i++)
                    {
                        columns[i] = metaData.getColumnLabel(i + 1);
                    }
                }
                catch (SQLException e)
                {
                    throw new RuntimeException("Error fetching columns from result set", e);
                }
            }

            private void setNextResultSet()
            {
                Connection connection = catalog.getConnection(context.getSession(), catalogAlias);
                try
                {
                    connection.setCatalog(database);
                    statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    if (parameters != null)
                    {
                        int size = parameters.size();
                        for (int i = 0; i < size; i++)
                        {
                            statement.setObject(i + 1, parameters.get(i));
                        }
                    }

                    if (statement.execute())
                    {
                        rs = statement.getResultSet();
                    }
                }
                catch (SQLException e)
                {
                    throw new RuntimeException("Error creating result set. (" + e.getMessage() + ")", e);
                }
            }
        };
    }
}