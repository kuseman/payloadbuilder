package org.kuse.payloadbuilder.catalog.jdbc;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.kuse.payloadbuilder.core.DescribeUtils.CATALOG;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.AOperator;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.IOrdinalValuesFactory.IOrdinalValues;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.InExpression;
import org.kuse.payloadbuilder.core.parser.LikeExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Jdbc Operator */
class JdbcOperator extends AOperator
{
    private final JdbcCatalog catalog;
    private final TableAlias tableAlias;
    private final String catalogAlias;
    private final List<AnalyzePair> predicatePairs;
    private final List<SortItem> sortItems;
    private final Index index;

    JdbcOperator(JdbcCatalog catalog, int nodeId, String catalogAlias, TableAlias tableAlias, List<AnalyzePair> predicatePairs, List<SortItem> sortItems, Index index)
    {
        super(nodeId);
        this.catalog = catalog;
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.predicatePairs = predicatePairs;
        this.sortItems = sortItems;
        this.index = index;
    }

    @Override
    public String getName()
    {
        return (index != null ? "index" : "scan") + " (" + tableAlias.getTable() + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        Map<String, Object> result = ofEntries(true,
                entry(CATALOG, JdbcCatalog.NAME));

        if (!isEmpty(predicatePairs))
        {
            result.put(PREDICATE, predicatePairs
                    .stream()
                    .map(p -> p.getPredicate().toString())
                    .collect(joining(" AND ")));
        }
        if (!isEmpty(sortItems))
        {
            result.put("Sort", sortItems
                    .stream()
                    .map(Objects::toString)
                    .collect(joining(",")));
        }

        return result;
    }

    @Override
    public TupleIterator open(ExecutionContext context)
    {
        return getIterator(catalog, context, catalogAlias, tableAlias, buildSql(context), null);
    }

    //CSOFF
    private String buildSql(ExecutionContext context)
    //CSON
    {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(tableAlias.getTableMeta() == null
            ? "*"
            : tableAlias.getTableMeta().getColumns().stream().map(c -> c.getName()).collect(joining(",")));
        sb.append(" FROM ");
        sb.append(tableAlias.getTable().toString()).append(" y ");

        boolean whereAdded = false;
        if (!predicatePairs.isEmpty())
        {
            for (AnalyzePair pair : predicatePairs)
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
                QualifiedName qname = pair.getQname(tableAlias.getAlias());
                Pair<Expression, Expression> expressionPair = pair.getExpressionPair(tableAlias.getAlias());
                //CSOFF
                switch (pair.getType())
                //CSON
                {
                    case UNDEFINED:
                        throw new IllegalArgumentException("Illegal predicate type");
                    case COMPARISION:
                        Object value = convertValue(expressionPair.getRight().eval(context));
                        sb.append(qname);
                        appendComparisonValue(sb, pair);
                        sb.append(value);
                        break;
                    case IN:
                        InExpression ie = (InExpression) pair.getRight().getExpression();
                        sb.append(qname);

                        if (ie.isNot())
                        {
                            sb.append(" NOT");
                        }

                        sb.append(" IN (");
                        List<Expression> arguments = ie.getArguments();
                        sb.append(arguments
                                .stream()
                                .map(e -> e.eval(context))
                                .filter(Objects::nonNull)
                                .map(o -> convertValue(o))
                                .collect(joining(",")));
                        sb.append(")");
                        break;
                    case LIKE:
                        LikeExpression le = (LikeExpression) pair.getRight().getExpression();
                        sb.append(qname);
                        sb.append(" LIKE ");
                        sb.append(convertValue(le.getPatternExpression().eval(context)));
                        break;
                    case NULL:
                        sb.append(qname);
                        sb.append(" IS NULL");
                        break;
                    case NOT_NULL:
                        sb.append(qname);
                        sb.append(" IS NOT NULL");
                        break;
                }
            }
        }

        if (index != null)
        {
            /* Build index sql from context values
             *
             * WHERE
             * (
             *    (col1 = 1 and col2 = 2)
             * or (col1 = 2 and col2 = 3)
             * )
             *
             */

            int size = index.getColumns().size();
            sb.append(whereAdded ? " AND (" : " WHERE (");
            Iterator<IOrdinalValues> it = context.getStatementContext().getOuterOrdinalValues();
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
                    sb.append(index.getColumns().get(i)).append("=");
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
            for (SortItem item : sortItems)
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

                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) item.getExpression();
                sb.append(qre.getQname().getLast());
                sb.append(item.getOrder() == Order.ASC ? " ASC" : " DESC");
            }
        }

        return sb.toString();
    }

    private String convertValue(Object value)
    {
        if (value instanceof Boolean)
        {
            return (Boolean) value ? "1" : "0";
        }
        else if (value instanceof String)
        {
            return "'" + String.valueOf(value) + "'";
        }

        return String.valueOf(value);
    }

    private void appendComparisonValue(StringBuilder sb, AnalyzePair pair)
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
    //CSOFF
    static TupleIterator getIterator(
            //CSOFF
            JdbcCatalog catalog,
            ExecutionContext context,
            String catalogAlias,
            TableAlias tableAlias,
            String query,
            List<Object> parameters)
    {
        final String database = context.getSession().getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE);

        //CSOFF
        return new TupleIterator()
        //CSON
        {
            private PreparedStatement statement;
            private ResultSet rs;
            private String[] columns;
            private int pos;

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

                return Row.of(tableAlias, pos, columns, values);
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
                    return rs != null && rs.next();
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
                        statement.getConnection().close();
                    }
                    catch (SQLException e)
                    {
                        throw new RuntimeException("Error closing result set", e);
                    }
                    statement = null;
                    rs = null;
                    columns = null;
                    pos = 0;
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
