package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.zaxxer.hikari.HikariDataSource;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/** Jdbc catalog */
public class JdbcCatalog extends Catalog
{
    public static final String NAME = "JdbcCatalog";
    public static final String DRIVER_CLASSNAME = "driverclassname";
    public static final String URL = "url";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASE = "database";
    public static final String SCHEMA = "schema";

    private final Map<String, HikariDataSource> dataSourceByURL = new ConcurrentHashMap<>();

    public JdbcCatalog()
    {
        super(NAME);
        registerFunction(new QueryFunction(this));
    }

    @Override
    public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        return new TableSchema(Schema.EMPTY, singletonList(new Index(table, emptyList(), Index.ColumnsType.WILDCARD)));
    }

    @Override
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        String type = table.getLast();

        if (SYS_TABLES.equalsIgnoreCase(type))
        {
            return new TableSchema(Schema.EMPTY);
        }
        else if (SYS_COLUMNS.equalsIgnoreCase(type))
        {
            return new TableSchema(Schema.EMPTY);
        }
        else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return new TableSchema(SYS_FUNCTIONS_SCHEMA);
        }

        return super.getSystemTableSchema(session, catalogAlias, table);
    }

    @Override
    public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        String type = table.getLast();

        if (SYS_TABLES.equalsIgnoreCase(type))
        {
            return (ctx, opt) -> getSystemIterator(session, catalogAlias, null, true);
        }
        else if (SYS_COLUMNS.equalsIgnoreCase(type))
        {
            final IExpression tableFilterExpression = data.extractEqualsPredicate(QualifiedName.of(SYS_COLUMNS_TABLE));
            return (ctx, opt) ->
            {
                String tableFilter = tableFilterExpression != null ? String.valueOf(tableFilterExpression.eval(ctx)
                        .valueAsObject(0))
                        : null;
                return getSystemIterator(session, catalogAlias, tableFilter, false);
            };
        }
        else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return (ctx, opt) -> TupleIterator.singleton(getFunctionsTupleVector(data.getSchema()
                    .get()));
        }

        throw new RuntimeException(type + " is not supported");
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        return getDataSource(session, catalogAlias, table, null, data);
    }

    @Override
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        return getDataSource(session, catalogAlias, seekPredicate.getIndex()
                .getTable(), seekPredicate, data);
    }

    private IDatasource getDataSource(IQuerySession session, String catalogAlias, QualifiedName table, ISeekPredicate seekPredicate, DatasourceData data)
    {
        List<IPredicate> predicates = getPredicates(data);
        List<ISortItem> sortItems = getSortItems(data);
        return new JdbcDatasource(this, catalogAlias, table, seekPredicate, data.getProjection(), predicates, sortItems);
    }

    private List<IPredicate> getPredicates(DatasourceData data)
    {
        List<IPredicate> pairs = new ArrayList<>();
        if (!data.getPredicates()
                .isEmpty())
        {
            Iterator<IPredicate> it = data.getPredicates()
                    .iterator();
            while (it.hasNext())
            {
                IPredicate pair = it.next();

                if (pair.getType() == IPredicate.Type.UNDEFINED)
                {
                    continue;
                }
                QualifiedName qname = pair.getQualifiedColumn();
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

    private List<ISortItem> getSortItems(DatasourceData data)
    {
        List<ISortItem> sortItems = emptyList();
        if (!data.getSortItems()
                .isEmpty()
                && data.getSortItems()
                        .stream()
                        .allMatch(i -> isApplicableSortItem(i)))
        {
            sortItems = new ArrayList<>(data.getSortItems());
            data.getSortItems()
                    .clear();
        }
        return sortItems;
    }

    private boolean isApplicableSortItem(ISortItem item)
    {
        if (item.getNullOrder() != NullOrder.UNDEFINED)
        {
            return false;
        }

        QualifiedName qname = item.getExpression()
                .getQualifiedColumn();
        if (qname == null)
        {
            return false;
        }

        // One part qnames are only supported
        return qname.getParts()
                .size() == 1;
    }

    /** Get connection for provided session/catalog alias */
    Connection getConnection(IQuerySession session, String catalogAlias)
    {
        final String driverClassName = session.getCatalogProperty(catalogAlias, JdbcCatalog.DRIVER_CLASSNAME)
                .valueAsString(0);
        final String url = session.getCatalogProperty(catalogAlias, JdbcCatalog.URL)
                .valueAsString(0);
        if (isBlank(url))
        {
            throw new IllegalArgumentException("Missing URL in catalog properties for " + catalogAlias);
        }

        final String username = session.getCatalogProperty(catalogAlias, JdbcCatalog.USERNAME)
                .valueAsString(0);
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
                    ds.setPoolName((url.length() > 40 ? url.substring(0, 40)
                            : url).replace(':', '_')
                            .replace('=', '_'));
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
        Object obj = session.getCatalogProperty(catalogAlias, JdbcCatalog.PASSWORD)
                .valueAsObject(0);
        if (obj instanceof String
                || obj instanceof UTF8String)
        {
            return String.valueOf(obj);
        }
        else if (obj instanceof char[])
        {
            return new String((char[]) obj);
        }
        return null;
    }

    private TupleIterator getSystemIterator(IQuerySession session, String catalogAlias, String tableFilter, boolean tables)
    {
        String database = session.getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE)
                .valueAsString(0);
        String schema = session.getCatalogProperty(catalogAlias, JdbcCatalog.SCHEMA)
                .valueAsString(0);
        Connection connection = null;
        ResultSet rs = null;
        try
        {
            connection = getConnection(session, catalogAlias);
            if (tables)
            {
                rs = connection.getMetaData()
                        .getTables(database, schema, null, null);
            }
            else
            {
                rs = connection.getMetaData()
                        .getColumns(database, schema, tableFilter, null);
            }
            int count = rs.getMetaData()
                    .getColumnCount();
            String[] columns = new String[count];
            int[] ordinals = new int[count];
            int index = tables ? 1
                    : 2;
            for (int i = 0; i < count; i++)
            {
                String columnName = rs.getMetaData()
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

            Schema plbSchema = new Schema(Arrays.stream(columns)
                    .map(c -> Column.of(c, Type.String))
                    .collect(toList()));

            List<Object[]> rows = new ArrayList<>();
            while (rs.next())
            {
                Object[] values = new Object[count];
                for (int i = 0; i < count; i++)
                {
                    values[i] = rs.getString(ordinals[i]);
                }

                rows.add(values);
            }

            return TupleIterator.singleton(new ObjectTupleVector(plbSchema, rows.size(), (row, col) -> rows.get(row)[col]));
        }
        catch (Exception e)
        {
            Utils.closeQuiet(connection, rs);
            throw new RuntimeException("Error listing tables", e);
        }
    }

    /** Shuts down catalog. Terminating pools etc. */
    void shutdown()
    {
        dataSourceByURL.values()
                .forEach(ds -> ds.close());
    }
}
