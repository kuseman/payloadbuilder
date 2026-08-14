package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.ProjectionType;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Catalog for querying MongoDB. Tables are addressed as {@code <database>.<collection>}. Read only - no insert/select into support. */
public class MongoCatalog extends Catalog
{
    public static final String NAME = "MongoCatalog";
    public static final String CONNECTIONSTRING_KEY = "connectionString";
    public static final String AUTH_USERNAME_KEY = "authUsername";
    public static final String AUTH_PASSWORD_KEY = "authPassword";
    public static final String AUTH_DATABASE_KEY = "authDatabase";
    public static final String CACHE_META_TTL_KEY = "cache.meta.ttl";
    public static final String CONNECT_TIMEOUT_KEY = "connectTimeout";
    public static final String SOCKET_TIMEOUT_KEY = "socketTimeout";
    public static final String SERVER_SELECTION_TIMEOUT_KEY = "serverSelectionTimeout";
    static final String ID_COLUMN = "_id";
    /** MongoDB's own internal databases - never surfaced through sys#tables/columns/indices. */
    private static final Set<String> SYSTEM_DATABASES = Set.of("admin", "local", "config");

    private final MongoClientHolder clientHolder = new MongoClientHolder();

    public MongoCatalog()
    {
        super(NAME);
        registerFunction(new AggregateFunction(this));
        registerFunction(new RunCommandFunction(this));
    }

    /** Return the (cached) {@link MongoClient} for provided session/catalog alias. Used by catalog functions like {@link AggregateFunction}/{@link RunCommandFunction}. */
    MongoClient getClient(IQuerySession session, String catalogAlias)
    {
        return clientHolder.getClient(session, catalogAlias);
    }

    @Override
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        MongoTable mongoTable = MongoTable.of(table);
        MongoClient client = clientHolder.getClient(context.getSession(), catalogAlias);
        return new TableSchema(Schema.EMPTY, getIndices(context.getSession(), catalogAlias, client, mongoTable, table));
    }

    private List<Index> getIndices(IQuerySession session, String catalogAlias, MongoClient client, MongoTable mongoTable, QualifiedName table)
    {
        List<Index> result = new ArrayList<>();
        // Every collection supports a point lookup on its primary key
        result.add(new Index(table, singletonList(ID_COLUMN), Index.ColumnsType.ALL));
        for (MongoMetaUtils.MongoIndex index : MongoMetaUtils.getIndexes(session, catalogAlias, client, mongoTable))
        {
            result.add(new Index(table, singletonList(index.column()), Index.ColumnsType.ALL));
        }
        return result;
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        return getDatasource(session, catalogAlias, table, null, data);
    }

    @Override
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        return getDatasource(session, catalogAlias, seekPredicate.getIndex()
                .getTable(), seekPredicate, data);
    }

    private IDatasource getDatasource(IQuerySession session, String catalogAlias, QualifiedName table, ISeekPredicate seekPredicate, DatasourceData data)
    {
        MongoTable mongoTable = MongoTable.of(table);

        List<IPredicate> predicates = MongoFilterBuilder.collect(data.getPredicates());
        Bson sort = MongoSortBuilder.build(data.getSortItems());
        Bson projection = MongoProjectionBuilder.build(data.getProjection());
        List<String> projectedColumns = data.getProjection()
                .type() == ProjectionType.COLUMNS ? data.getProjection()
                        .columns()
                        : null;

        return new MongoDatasource(data.getNodeId(), clientHolder, catalogAlias, mongoTable, seekPredicate, predicates, sort, projection, projectedColumns, data.getOptions());
    }

    @Override
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        String type = table.getLast();
        if (SYS_TABLES.equalsIgnoreCase(type)
                || SYS_COLUMNS.equalsIgnoreCase(type)
                || SYS_INDICES.equalsIgnoreCase(type))
        {
            return TableSchema.EMPTY;
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
            return context -> getTablesIterator(session, catalogAlias);
        }
        else if (SYS_COLUMNS.equalsIgnoreCase(type))
        {
            return context -> getColumnsIterator(session, catalogAlias);
        }
        else if (SYS_INDICES.equalsIgnoreCase(type))
        {
            return context -> getIndicesIterator(session, catalogAlias);
        }
        else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return context -> TupleIterator.singleton(getFunctionsTupleVector(SYS_FUNCTIONS_SCHEMA));
        }

        throw new RuntimeException(table + " is not supported");
    }

    private TupleIterator getTablesIterator(IQuerySession session, String catalogAlias)
    {
        MongoClient client = clientHolder.getClient(session, catalogAlias);
        List<Object[]> rows = new ArrayList<>();
        for (String database : client.listDatabaseNames())
        {
            if (SYSTEM_DATABASES.contains(database))
            {
                continue;
            }
            for (String collection : MongoMetaUtils.listCollectionNames(client, database))
            {
                rows.add(new Object[] { database + "." + collection });
            }
        }

        Schema schema = new Schema(singletonList(Column.of(SYS_TABLES_NAME, Type.String)));
        return TupleIterator.singleton(new ObjectTupleVector(schema, rows.size(), (row, col) -> rows.get(row)[col]));
    }

    /**
     * Return columns per table: '_id' plus indexed fields only, sourced from the same cached index metadata used for seek/sys#indices. Deliberately does not sample document bodies - that's unbounded
     * cost against a realistically sized cluster; full schema discovery belongs in a dedicated editor/completion service instead.
     */
    private TupleIterator getColumnsIterator(IQuerySession session, String catalogAlias)
    {
        MongoClient client = clientHolder.getClient(session, catalogAlias);
        List<Object[]> rows = new ArrayList<>();
        for (String database : client.listDatabaseNames())
        {
            if (SYSTEM_DATABASES.contains(database))
            {
                continue;
            }
            for (String collectionName : MongoMetaUtils.listCollectionNames(client, database))
            {
                String table = database + "." + collectionName;
                MongoTable mongoTable = new MongoTable(database, collectionName);

                Set<String> columns = new LinkedHashSet<>();
                columns.add(ID_COLUMN);
                for (MongoMetaUtils.MongoIndex index : MongoMetaUtils.getIndexes(session, catalogAlias, client, mongoTable))
                {
                    columns.add(index.column());
                }

                for (String column : columns)
                {
                    rows.add(new Object[] { table, column });
                }
            }
        }

        Schema schema = new Schema(List.of(Column.of(SYS_COLUMNS_TABLE, Type.String), Column.of(SYS_COLUMNS_NAME, Type.String)));
        return TupleIterator.singleton(new ObjectTupleVector(schema, rows.size(), (row, col) -> rows.get(row)[col]));
    }

    private TupleIterator getIndicesIterator(IQuerySession session, String catalogAlias)
    {
        MongoClient client = clientHolder.getClient(session, catalogAlias);
        List<Object[]> rows = new ArrayList<>();
        for (String database : client.listDatabaseNames())
        {
            if (SYSTEM_DATABASES.contains(database))
            {
                continue;
            }
            for (String collectionName : MongoMetaUtils.listCollectionNames(client, database))
            {
                String table = database + "." + collectionName;
                rows.add(new Object[] { table, singletonList(ID_COLUMN) });

                MongoTable mongoTable = new MongoTable(database, collectionName);
                for (MongoMetaUtils.MongoIndex index : MongoMetaUtils.getIndexes(session, catalogAlias, client, mongoTable))
                {
                    rows.add(new Object[] { table, singletonList(index.column()) });
                }
            }
        }

        Schema schema = new Schema(List.of(Column.of(SYS_INDICES_TABLE, Type.String), Column.of(SYS_INDICES_COLUMNS, Type.Any)));
        return TupleIterator.singleton(new ObjectTupleVector(schema, rows.size(), (row, col) -> rows.get(row)[col]));
    }

    /** Shuts down catalog, closing cached Mongo clients. */
    @Override
    public void close()
    {
        clientHolder.close();
    }
}
