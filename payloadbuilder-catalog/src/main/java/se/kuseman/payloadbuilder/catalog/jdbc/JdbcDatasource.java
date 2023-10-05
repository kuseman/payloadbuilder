package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.SeekType;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/** Jdbc datasource */
class JdbcDatasource implements IDatasource
{
    private final JdbcCatalog catalog;
    private final String catalogAlias;
    private final QualifiedName table;
    private final List<String> projection;
    private final List<IPredicate> predicates;
    private final List<ISortItem> sortItems;
    private final ISeekPredicate indexPredicate;

    JdbcDatasource(JdbcCatalog catalog, String catalogAlias, QualifiedName table, ISeekPredicate indexPredicate, List<String> projection, List<IPredicate> predicates, List<ISortItem> sortItems)
    {
        this.catalog = catalog;
        this.catalogAlias = catalogAlias;
        this.table = table;
        this.indexPredicate = indexPredicate;
        this.predicates = predicates;
        this.sortItems = sortItems;
        this.projection = projection;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> result = ofEntries(true, entry(CATALOG, JdbcCatalog.NAME));

        if (!isEmpty(predicates))
        {
            result.put(PREDICATE, predicates.stream()
                    .map(IPredicate::getSqlRepresentation)
                    .collect(joining(" AND ")));
        }
        if (!isEmpty(sortItems))
        {
            result.put("Sort", sortItems.stream()
                    .map(Objects::toString)
                    .collect(joining(",")));
        }

        result.put("Query", buildSql(context, true));

        return result;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
    {
        String sql = buildSql(context, false);
        return getIterator(catalog, context, catalogAlias, sql, null, options.getBatchSize(context));
    }

    // CSOFF
    private String buildSql(IExecutionContext context, boolean describe)
    // CSON
    {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(projection.isEmpty() ? "y.*"
                : projection.stream()
                        .map(c -> "y." + c)
                        .collect(joining(",")));
        sb.append(" FROM ");
        sb.append(table.toString())
                .append(" y");

        if (indexPredicate != null)
        {
            /*
             * Build index sql from context values
             * @formatter:off
             * INNER JOIN
             * (
             *         SELECT 1 col1, 2 col2
             *   UNION SELECT 2,      4
             *   UNION SELECT 3,      6
             * 
             * ) xx
             *   ON xx.col1 = y.col1
             *   AND xx.col2 = y.col2
             * @formatter:on
             *
             */

            sb.append(" INNER JOIN (");

            List<ISeekKey> seekKeys;
            if (describe)
            {
                // Create dummy values
                seekKeys = indexPredicate.getIndexColumns()
                        .stream()
                        .map(c -> new ISeekKey()
                        {
                            @Override
                            public ValueVector getValue()
                            {
                                return ValueVector.literalString("<index values " + c + ">", 1);
                            }

                            @Override
                            public SeekType getType()
                            {
                                return SeekType.EQ;
                            }
                        })
                        .collect(toList());
            }
            else
            {
                seekKeys = indexPredicate.getSeekKeys(context);
            }

            int keySize = seekKeys.size();
            int rowCount = seekKeys.get(0)
                    .getValue()
                    .size();
            for (int i = 0; i < rowCount; i++)
            {
                if (i > 0)
                {
                    sb.append(" UNION ");
                }

                sb.append("SELECT ");

                for (int j = 0; j < keySize; j++)
                {
                    ValueVector values = seekKeys.get(j)
                            .getValue();
                    if (j > 0)
                    {
                        sb.append(", ");
                    }
                    Object value = convertValue(values.valueAsObject(i));
                    sb.append(value);
                    if (i == 0)
                    {
                        // Name columns on first row
                        sb.append(" ")
                                .append(indexPredicate.getIndexColumns()
                                        .get(j));
                    }
                }
            }

            sb.append(") xx ON ");
            for (int i = 0; i < keySize; i++)
            {
                if (i > 0)
                {
                    sb.append(" AND ");
                }

                String indexCol = indexPredicate.getIndexColumns()
                        .get(i);
                sb.append("xx.")
                        .append(indexCol)
                        .append(" = y.")
                        .append(indexCol);
            }
        }

        if (!predicates.isEmpty())
        {
            sb.append(" WHERE ");
            boolean first = true;
            for (IPredicate predicate : predicates)
            {
                if (!first)
                {
                    sb.append(" AND ");
                    first = false;
                }
                QualifiedName qname = predicate.getQualifiedColumn();
                switch (predicate.getType())
                {
                    case COMPARISION:
                        Object value = convertValue(predicate.getComparisonExpression()
                                .eval(context)
                                .valueAsObject(0));
                        sb.append("y.")
                                .append(qname.toString());
                        appendComparisonValue(sb, predicate);
                        sb.append(value);
                        break;
                    case IN:
                        IInExpression inExpression = predicate.getInExpression();
                        sb.append("y.")
                                .append(qname.toString());

                        if (inExpression.isNot())
                        {
                            sb.append(" NOT");
                        }

                        sb.append(" IN (");
                        sb.append(inExpression.getArguments()
                                .stream()
                                .map(e -> e.eval(context)
                                        .valueAsObject(0))
                                .filter(Objects::nonNull)
                                .map(o -> convertValue(o))
                                .collect(joining(",")));
                        sb.append(")");
                        break;
                    case LIKE:
                        ILikeExpression likeExpression = predicate.getLikeExpression();

                        if (likeExpression.isNot())
                        {
                            sb.append(" NOT");
                        }

                        IExpression valueSupplier = likeExpression.getPatternExpression();
                        sb.append("y.")
                                .append(qname.toString());
                        sb.append(" LIKE ");
                        sb.append(convertValue(valueSupplier.eval(context)
                                .valueAsObject(0)));
                        break;
                    case NULL:
                        INullPredicateExpression nullExpression = predicate.getNullPredicateExpression();

                        sb.append("y.")
                                .append(qname.toString());
                        sb.append(" IS ");

                        if (nullExpression.isNot())
                        {
                            sb.append(" NOT");
                        }

                        sb.append(" NULL");
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal predicate type");
                }
            }
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

                sb.append("y.")
                        .append(item.getExpression()
                                .getQualifiedColumn()
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
            value = (Boolean) value ? 1
                    : 0;
        }

        if (!(value instanceof Number))
        {
            return "'" + String.valueOf(value) + "'";
        }

        return String.valueOf(value);
    }

    private void appendComparisonValue(StringBuilder sb, IPredicate predicate)
    {
        switch (predicate.getComparisonType())
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
    static TupleIterator getIterator(JdbcCatalog catalog, IExecutionContext context, String catalogAlias, String query, List<Object> parameters, int batchSize)
    {
        final String database = context.getSession()
                .getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE)
                .valueAsString(0);
        final String schema = context.getSession()
                .getCatalogProperty(catalogAlias, JdbcCatalog.SCHEMA)
                .valueAsString(0);

        // CSOFF
        return new TupleIterator()
        // CSON
        {
            private AbortRunnable abortRunnable = new AbortRunnable();
            private Connection connection;
            private PreparedStatement statement;
            private ResultSet rs;
            private String[] columns;
            private int[] jdbcTypes;
            private boolean resultSetEnded = false;
            private volatile boolean abort = false;

            {
                context.getSession()
                        .registerAbortListener(abortRunnable);
            }

            class AbortRunnable implements Runnable
            {
                @Override
                public void run()
                {
                    abort = true;
                    Utils.cancelQuiet(statement);
                }
            }

            @Override
            public TupleVector next()
            {
                try
                {
                    if (columns == null)
                    {
                        populateMeta();
                    }

                    List<Object[]> batch = new ArrayList<>(batchSize);
                    int length = columns.length;
                    do
                    {
                        Object[] values = new Object[length];
                        for (int i = 0; i < length; i++)
                        {
                            if (abort)
                            {
                                break;
                            }
                            values[i] = Utils.getAndConvertValue(rs, i + 1, jdbcTypes[i]);
                        }

                        if (abort)
                        {
                            break;
                        }

                        batch.add(values);

                        if (batch.size() >= batchSize)
                        {
                            break;
                        }

                        resultSetEnded = !rs.next();
                    } while (!resultSetEnded);

                    Utils.printWarnings(rs, context.getSession());
                    Utils.printWarnings(statement, context.getSession());
                    Utils.printWarnings(connection, context.getSession());

                    Schema schema = new Schema(Arrays.stream(columns)
                            .map(c -> Column.of(c, Type.Any))
                            .collect(toList()));
                    return new ObjectTupleVector(schema, batch.size(), (row, col) -> batch.get(row)[col]);
                }
                catch (Exception e)
                {
                    // Close everything here just to be safe
                    // close on iterator should be called but better safe than sorry
                    Utils.closeQuiet(connection, statement, rs);
                    throw new RuntimeException("Error fetching value from result set", e);
                }
            }

            @Override
            public boolean hasNext()
            {
                while (true)
                {
                    if (abort)
                    {
                        return false;
                    }

                    try
                    {
                        if (rs == null)
                        {
                            rs = setNextResultSet();
                            // No more result, we're done
                            if (rs == null)
                            {
                                return false;
                            }

                            // Set result set at row 1
                            resultSetEnded = !rs.next();
                        }

                        // Current result set ended, fetch next
                        if (resultSetEnded)
                        {
                            columns = null;
                            jdbcTypes = null;
                            rs = null;
                        }
                        else
                        {
                            return true;
                        }
                    }
                    catch (Exception e)
                    {
                        // Close everything here just to be safe
                        // close on iterator should be called but better safe than sorry
                        Utils.closeQuiet(connection, statement, rs);
                        throw new RuntimeException("Error advancing result set", e);
                    }
                }
            }

            @Override
            public void close()
            {
                Utils.printWarnings(rs, context.getSession());
                Utils.printWarnings(statement, context.getSession());
                Utils.printWarnings(connection, context.getSession());
                Utils.closeQuiet(connection, statement, rs);
                columns = null;
                jdbcTypes = null;
                rs = null;
                statement = null;
                connection = null;
            }

            private void populateMeta() throws SQLException
            {
                ResultSetMetaData metaData = rs.getMetaData();
                int count = metaData.getColumnCount();
                columns = new String[count];
                jdbcTypes = new int[count];

                for (int i = 0; i < count; i++)
                {
                    columns[i] = metaData.getColumnLabel(i + 1);
                    jdbcTypes[i] = metaData.getColumnType(i + 1);
                }
            }

            private ResultSet setNextResultSet() throws Exception
            {
                boolean first = connection == null;
                // First result set, open connection an create statement
                if (connection == null)
                {
                    connection = catalog.getConnection(context.getSession(), catalogAlias);
                    Utils.printWarnings(connection, context.getSession());
                    if (!isBlank(database))
                    {
                        connection.setCatalog(database);
                    }
                    if (!isBlank(schema))
                    {
                        connection.setSchema(schema);
                    }
                    statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    Utils.printWarnings(statement, context.getSession());
                    if (parameters != null)
                    {
                        int size = parameters.size();
                        for (int i = 0; i < size; i++)
                        {
                            statement.setObject(i + 1, parameters.get(i));
                        }
                    }
                }

                // Skip a while loop here to protect against bugs/bad drivers etc.
                // Traverse until we have a result set or there are no more result sets
                for (int iteration = 0; iteration < 256; iteration++)
                {
                    try
                    {
                        boolean isResultSet = first ? statement.execute()
                                : statement.getMoreResults();
                        first = false;
                        Utils.printWarnings(statement, context.getSession());
                        if (isResultSet)
                        {
                            ResultSet rs = statement.getResultSet();
                            Utils.printWarnings(statement, context.getSession());
                            return rs;
                        }
                        else
                        {
                            int updateCount = statement.getUpdateCount();
                            // We're done
                            if (updateCount < 0)
                            {
                                return null;
                            }

                            context.getSession()
                                    .getPrintWriter()
                                    .append(String.valueOf(updateCount))
                                    .append(" row(s) affected")
                                    .append(System.lineSeparator());
                        }
                    }
                    catch (SQLException e)
                    {
                        context.getSession()
                                .handleKnownException(e);
                    }
                    finally
                    {
                        first = false;
                    }
                }

                throw new RuntimeException("Max iteration count reached when trying to fetch a result set.");
            }
        };
    }
}
