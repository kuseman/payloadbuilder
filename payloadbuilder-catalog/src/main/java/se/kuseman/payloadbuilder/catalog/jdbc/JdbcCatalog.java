package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.zaxxer.hikari.HikariDataSource;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.session.IQuerySession;

/** Jdbc catalog */
public class JdbcCatalog extends Catalog
{
    private static final int BATCH_SIZE = 500;
    public static final String NAME = "JdbcCatalog";
    static final String DRIVER_CLASSNAME = "driverclassname";
    static final String URL = "url";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String DATABASE = "database";

    private final Map<String, HikariDataSource> dataSourceByURL = new ConcurrentHashMap<>();

    public JdbcCatalog()
    {
        super(NAME);
        registerFunction(new QueryFunction(this));
    }

    @Override
    public List<Index> getIndices(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        return singletonList(new Index(table, emptyList(), Index.ColumnsType.WILDCARD, BATCH_SIZE));
    }

    @Override
    public Operator getSystemOperator(OperatorData data)
    {
        final IQuerySession session = data.getSession();
        final String catalogAlias = data.getCatalogAlias();
        final TableAlias alias = data.getTableAlias();
        String type = alias.getTable()
                .getLast();

        if (SYS_TABLES.equalsIgnoreCase(type))
        {
            return systemOperator(data.getNodeId(), type, ctx -> getTupleIterator(session, catalogAlias, alias, null, true));
        }
        else if (SYS_COLUMNS.equalsIgnoreCase(type))
        {
            IExpression tableFilter = data.extractPredicate(SYS_COLUMNS_TABLE);
            return systemOperator(data.getNodeId(), type, ctx ->
            {
                String table = tableFilter != null ? String.valueOf(tableFilter.eval(ctx))
                        : null;
                return getTupleIterator(session, catalogAlias, alias, table, false);
            });
        }
        else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return getFunctionsOperator(data.getNodeId(), alias);
        }

        throw new RuntimeException(type + " is not supported");
    }

    @Override
    public Operator getScanOperator(OperatorData data)
    {
        return getIndexOperator(data, null);
    }

    @Override
    public Operator getIndexOperator(OperatorData data, IIndexPredicate indexPredicate)
    {
        List<IAnalyzePair> pairs = getPredicatePairs(data);
        List<ISortItem> sortItems = getSortItems(data);
        return new JdbcOperator(this, data.getNodeId(), data.getCatalogAlias(), data.getTableAlias(), pairs, sortItems, indexPredicate);
    }

    private List<IAnalyzePair> getPredicatePairs(OperatorData data)
    {
        List<IAnalyzePair> pairs = new ArrayList<>();
        if (!data.getPredicatePairs()
                .isEmpty())
        {
            Iterator<IAnalyzePair> it = data.getPredicatePairs()
                    .iterator();
            while (it.hasNext())
            {
                IAnalyzePair pair = it.next();

                if (pair.getType() == Type.UNDEFINED)
                {
                    continue;
                }
                QualifiedName qname = pair.getQualifiedName(data.getTableAlias()
                        .getAlias());
                if (qname == null
                        || qname.getParts()
                                .size() > 2)
                {
                    continue;
                }

                pairs.add(pair);
                it.remove();
            }
        }
        return pairs;
    }

    private List<ISortItem> getSortItems(OperatorData data)
    {
        List<ISortItem> sortItems = emptyList();
        if (!data.getSortItems()
                .isEmpty()
                && data.getSortItems()
                        .stream()
                        .allMatch(i -> isApplicableSortItem(data.getTableAlias(), i)))
        {
            sortItems = new ArrayList<>(data.getSortItems());
            data.getSortItems()
                    .clear();
        }
        return sortItems;
    }

    private boolean isApplicableSortItem(TableAlias tableAlias, ISortItem item)
    {
        if (item.getNullOrder() != NullOrder.UNDEFINED)
        {
            return false;
        }

        QualifiedName qname = item.getExpression()
                .getQualifiedName();
        if (qname == null)
        {
            return false;
        }

        // One part qname or 2 part where alias is matching
        return qname.getParts()
                .size() == 1
                || (qname.getParts()
                        .size() == 2
                        && equalsIgnoreCase(tableAlias.getAlias(), qname.getParts()
                                .get(0)));
    }

    /** Get connection for provided session/catalog alias */
    Connection getConnection(IQuerySession session, String catalogAlias)
    {
        final String driverClassName = session.getCatalogProperty(catalogAlias, JdbcCatalog.DRIVER_CLASSNAME);
        final String url = session.getCatalogProperty(catalogAlias, JdbcCatalog.URL);
        if (isBlank(url))
        {
            throw new IllegalArgumentException("Missing URL in catalog properties for " + catalogAlias);
        }

        final String username = session.getCatalogProperty(catalogAlias, JdbcCatalog.USERNAME);
        final String password = getPassword(session, catalogAlias);
        if (isBlank(username)
                || isBlank(password))
        {
            throw new CredentialsException(catalogAlias, "Missing username/password in catalog properties for " + catalogAlias);
        }

        return getConnection(driverClassName, url, username, password, catalogAlias);
    }

    /** Get connection for provided url and user/pass */
    Connection getConnection(String driverClassName, String url, String username, String password, String catalogAlias)
    {
        try
        {
            return dataSourceByURL.compute(url, (k, v) ->
            {
                if (v == null)
                {
                    HikariDataSource ds = new HikariDataSource();
                    if (isNotBlank(driverClassName))
                    {
                        ds.setDriverClassName(driverClassName);
                    }
                    ds.setRegisterMbeans(true);
                    // CSOFF
                    ds.setPoolName(url.replace(':', '_')
                            .substring(0, 40));
                    // CSON
                    ds.setJdbcUrl(url);
                    ds.setUsername(username);
                    ds.setPassword(password);
                    return ds;
                }

                // Set user/pass in case they have changed
                v.setUsername(username);
                v.setPassword(password);
                return v;
            })
                    .getConnection();
        }
        catch (SQLException e)
        {
            throw new ConnectionException(catalogAlias, e);
        }
    }

    private String getPassword(IQuerySession session, String catalogAlias)
    {
        Object obj = session.getCatalogProperty(catalogAlias, JdbcCatalog.PASSWORD);
        if (obj instanceof String)
        {
            return (String) obj;
        }
        else if (obj instanceof char[])
        {
            return new String((char[]) obj);
        }
        return null;
    }

    private TupleIterator getTupleIterator(IQuerySession session, String catalogAlias, TableAlias tableAlias, String tableFilter, boolean tables)
    {
        String database = session.getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE);
        AtomicReference<Connection> connection = new AtomicReference<>();
        AtomicReference<ResultSet> rs = new AtomicReference<>();
        try
        {
            connection.set(getConnection(session, catalogAlias));
            if (tables)
            {
                rs.set(connection.get()
                        .getMetaData()
                        .getTables(database, null, null, null));
            }
            else
            {
                rs.set(connection.get()
                        .getMetaData()
                        .getColumns(database, null, tableFilter, null));
            }
            final int count = rs.get()
                    .getMetaData()
                    .getColumnCount();
            final String[] columns = new String[count];
            final int[] ordinals = new int[count];
            int index = tables ? 1
                    : 2;
            for (int i = 0; i < count; i++)
            {
                String columnName = rs.get()
                        .getMetaData()
                        .getColumnName(i + 1);
                if ("TABLE_NAME".equalsIgnoreCase(columnName))
                {
                    columns[0] = tables ? SYS_TABLES_NAME
                            : SYS_COLUMNS_TABLE;
                    ordinals[0] = i + 1;
                }
                else if (!tables
                        && "COLUMN_NAME".equalsIgnoreCase(columnName))
                {
                    columns[1] = SYS_COLUMNS_NAME;
                    ordinals[1] = i + 1;
                }
                else
                {
                    ordinals[index] = i + 1;
                    columns[index++] = columnName;
                }
            }

            // CSOFF
            return new TupleIterator()
            // CSON
            {
                @Override
                public Tuple next()
                {
                    Object[] values = new Object[count];
                    try
                    {
                        for (int i = 0; i < count; i++)
                        {
                            values[i] = rs.get()
                                    .getString(ordinals[i]);
                        }
                    }
                    catch (SQLException e)
                    {
                        throw new RuntimeException("Error reading resultset", e);
                    }

                    return Row.of(tableAlias, columns, values);
                }

                @Override
                public boolean hasNext()
                {
                    try
                    {
                        return rs.get()
                                .next();
                    }
                    catch (SQLException e)
                    {
                        throw new RuntimeException("Error advancing resultset", e);
                    }
                }

                @Override
                public void close()
                {
                    Utils.closeQuiet(connection.get(), rs.get());
                }
            };
        }
        catch (Exception e)
        {
            Utils.closeQuiet(connection.get(), rs.get());
            throw new RuntimeException("Error listing tables", e);
        }
    }
}