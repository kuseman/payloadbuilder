package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.cache.Cache;
import se.kuseman.payloadbuilder.core.cache.Cache.CacheEntryInfo;
import se.kuseman.payloadbuilder.core.cache.CacheInfo;
import se.kuseman.payloadbuilder.core.cache.CacheType;
import se.kuseman.payloadbuilder.core.catalog.system.AMatchFunction.MatchType;
import se.kuseman.payloadbuilder.core.catalog.system.TrimFunction.Type;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;

/** Built in system catalog */
public class SystemCatalog extends Catalog
{
    public static final String NAME = "System";
    public static final String ALIAS = "sys";

    //@formatter:off
    private static final Schema CATALOGS_SCHEMA = Schema.of(
            Column.of("alias", ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("name",  ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)));
    
    private static final Schema FUNCTIONS_SCHEMA = Schema.of(
            Column.of(SYS_FUNCTIONS_NAME,        ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of(SYS_FUNCTIONS_TYPE,        ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of(SYS_FUNCTIONS_DESCRIPTION, ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)));
    
    private static final Schema TABLES_SCHEMA = Schema.of(
            Column.of(SYS_TABLES_NAME, ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("schema",        ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("rows",          ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Int)));
    
    private static final Schema CACHES_SCHEMA = Schema.of(
            Column.of("name",       ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("size",       ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Int)),
            Column.of("hits",       ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Int)),
            Column.of("hit_ratio",  ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Float)),
            Column.of("misses",     ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Int)),
            Column.of("miss_ratio", ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Float)),
            Column.of("type",       ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("provider",   ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)));
    
    private static final Schema CACHE_KEYS_SCHEMA = Schema.of(
            Column.of("name",        ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("key",         ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("expire_time", ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.Any)),
            Column.of("type",        ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("provider",    ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)));
    
    private static final Schema VARIABLES_SCHEMA = Schema.of(
            Column.of("name",  ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
            Column.of("value", ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)));
    //@formatter:on

    SystemCatalog()
    {
        super(NAME);
    }

    /** Get instance of system catalog */
    public static Catalog get()
    {
        SystemCatalog catalog = new SystemCatalog();

        // String functions
        catalog.registerFunction(new LowerUpperFunction(catalog, true));
        catalog.registerFunction(new LowerUpperFunction(catalog, false));
        catalog.registerFunction(new SubstringFunction(catalog));
        catalog.registerFunction(new TrimFunction(catalog, Type.BOTH));
        catalog.registerFunction(new TrimFunction(catalog, Type.LEFT));
        catalog.registerFunction(new TrimFunction(catalog, Type.RIGHT));
        catalog.registerFunction(new PadFunction(catalog, true));
        catalog.registerFunction(new PadFunction(catalog, false));
        catalog.registerFunction(new ReplaceFunction(catalog));
        catalog.registerFunction(new LengthFunction(catalog));
        catalog.registerFunction(new FormatFunction(catalog));

        // Date functions
        catalog.registerFunction(new GetDateFunction(catalog, true));
        catalog.registerFunction(new GetDateFunction(catalog, false));
        catalog.registerFunction(new UnixTimeStampFunction(catalog));

        // Scalar functions
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new DistinctFunction(catalog));
        catalog.registerFunction(new IsNullFunction(catalog));
        catalog.registerFunction(new IsBlankFunction(catalog));
        catalog.registerFunction(new CoalesceFunction(catalog));
        catalog.registerFunction(new JsonValueFunction(catalog));
        catalog.registerFunction(new RegexpLikeFunction(catalog));
        catalog.registerFunction(new RegexpMatchFunction(catalog));
        catalog.registerFunction(new TypeOfFunction(catalog));
        catalog.registerFunction(new ContainsFunction(catalog));

        // Misc functions
        catalog.registerFunction(new RandomInt(catalog));
        catalog.registerFunction(new ConcatFunction(catalog));
        catalog.registerFunction(new ListOfFunction(catalog));
        catalog.registerFunction(new ToListFunction(catalog));

        // Lambda functions
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new FlatMapFunction(catalog));
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.ANY));
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.ALL));
        catalog.registerFunction(new AMatchFunction(catalog, MatchType.NONE));

        // Table functions
        catalog.registerFunction(new RangeFunction(catalog));
        catalog.registerFunction(new StringSplitFunction(catalog));
        catalog.registerFunction(new OpenMapCollectionFunction(catalog));

        // Aggregate functions
        catalog.registerFunction(new AggregateAvgFunction(catalog));
        catalog.registerFunction(new AggregateMinFunction(catalog));
        catalog.registerFunction(new AggregateMaxFunction(catalog));
        catalog.registerFunction(new AggregateCountFunction(catalog));
        catalog.registerFunction(new AggregateSumFunction(catalog));
        catalog.registerFunction(new AggregateStructureValueFunction(catalog, AStructureValueFunction.ARRAY));
        catalog.registerFunction(new AggregateStructureValueFunction(catalog, AStructureValueFunction.OBJECT_ARRAY));
        catalog.registerFunction(new AggregateStructureValueFunction(catalog, AStructureValueFunction.OBJECT));

        // Operator functions
        catalog.registerFunction(new OperatorStructureValueFunction(catalog, AStructureValueFunction.ARRAY));
        catalog.registerFunction(new OperatorStructureValueFunction(catalog, AStructureValueFunction.OBJECT_ARRAY));
        catalog.registerFunction(new OperatorStructureValueFunction(catalog, AStructureValueFunction.OBJECT));

        return catalog;
    }

    @Override
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        if (table.size() == 1)
        {
            String type = table.getLast();
            if ("catalogs".equalsIgnoreCase(type))
            {
                return new TableSchema(CATALOGS_SCHEMA);
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return new TableSchema(FUNCTIONS_SCHEMA);
            }
            else if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return new TableSchema(TABLES_SCHEMA);
            }
            else if ("caches".equalsIgnoreCase(type))
            {
                return new TableSchema(CACHES_SCHEMA);
            }
            else if ("cachekeys".equalsIgnoreCase(type))
            {
                return new TableSchema(CACHE_KEYS_SCHEMA);
            }
            else if ("variables".equalsIgnoreCase(type))
            {
                return new TableSchema(VARIABLES_SCHEMA);
            }
        }

        throw new RuntimeException("System table: " + table + " is not supported");
    }

    @Override
    public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        Function<IExecutionContext, TupleVector> vector = null;
        if (table.size() == 1)
        {
            String type = table.getLast();
            if ("catalogs".equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    final List<Entry<String, Catalog>> catalogs = new ArrayList<>(((QuerySession) ctx.getSession()).getCatalogRegistry()
                            .getCatalogs());
                    return new ObjectTupleVector(CATALOGS_SCHEMA, catalogs.size(), (row, col) ->
                    {
                        Entry<String, Catalog> e = catalogs.get(row);
                        if (col == 0)
                        {
                            return e.getKey();
                        }

                        return e.getValue()
                                .getName();
                    });
                };

            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                vector = ctx -> getFunctionsTupleVector();
            }
            else if (SYS_TABLES.equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    final List<Entry<String, TupleVector>> tables = new ArrayList<>(((QuerySession) ctx.getSession()).getTemporaryTables());
                    return new ObjectTupleVector(TABLES_SCHEMA, tables.size(), (row, col) ->
                    {
                        Entry<String, TupleVector> e = tables.get(row);
                        if (col == 0)
                        {
                            return e.getKey();
                        }
                        else if (col == 1)
                        {
                            return e.getValue()
                                    .getSchema()
                                    .getColumns()
                                    .stream()
                                    .map(c -> c.getName() + " (" + c.getType() + ")")
                                    .collect(joining(", "));
                        }

                        return e.getValue()
                                .getRowCount();
                    });

                };
            }
            else if ("caches".equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    QuerySession querySession = (QuerySession) ctx.getSession();
                    List<Pair<Cache, CacheInfo>> caches = asList(querySession.getBatchCache(), querySession.getTempTableCache(), querySession.getGenericCache()).stream()
                            .flatMap(p -> p.getCaches()
                                    .stream()
                                    .map(c -> Pair.of(p, c)))
                            .collect(toList());
                    return new ObjectTupleVector(CACHES_SCHEMA, caches.size(), (row, col) ->
                    {
                        Pair<Cache, CacheInfo> pair = caches.get(row);
                        // CSOFF
                        switch (col)
                        // CSON
                        {
                            case 0:
                                return pair.getValue()
                                        .getName();
                            case 1:
                                return pair.getValue()
                                        .getSize();
                            case 2:
                                return pair.getValue()
                                        .getCacheHits();
                            case 3:
                                return (float) pair.getValue()
                                        .getCacheHits()
                                        / (pair.getValue()
                                                .getCacheHits()
                                                + pair.getValue()
                                                        .getCacheMisses());
                            case 4:
                                return pair.getValue()
                                        .getCacheMisses();
                            case 5:
                                return (float) pair.getValue()
                                        .getCacheMisses()
                                        / (pair.getValue()
                                                .getCacheHits()
                                                + pair.getValue()
                                                        .getCacheMisses());
                            case 6:
                                return CacheType.from(pair.getKey());
                            case 7:
                                return pair.getKey()
                                        .getName();
                        }

                        throw new IllegalArgumentException("Illegal column index: " + col);
                    });

                };
            }
            else if ("cachekeys".equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    QuerySession querySession = (QuerySession) ctx.getSession();

                    final List<Pair<Pair<Cache, CacheInfo>, CacheEntryInfo>> cacheKeys = asList(querySession.getBatchCache(), querySession.getTempTableCache(), querySession.getGenericCache()).stream()
                            .flatMap(p -> p.getCaches()
                                    .stream()
                                    .map(c -> Pair.of(p, c)))
                            .flatMap(p -> p.getKey()
                                    .getCacheEntries(p.getValue()
                                            .getName())
                                    .stream()
                                    .map(ce -> Pair.of(p, ce)))
                            .collect(toList());

                    return new ObjectTupleVector(CACHE_KEYS_SCHEMA, cacheKeys.size(), (row, col) ->
                    {
                        Pair<Pair<Cache, CacheInfo>, CacheEntryInfo> p = cacheKeys.get(row);
                        // CSOFF
                        switch (col)
                        // CSON
                        {
                            case 0:
                                return p.getKey()
                                        .getValue()
                                        .getName();
                            case 1:
                                return p.getValue()
                                        .getKey();
                            case 2:
                                return p.getValue()
                                        .getExpireTime();
                            case 3:
                                return CacheType.from(p.getKey()
                                        .getKey());
                            case 4:
                                return p.getKey()
                                        .getKey()
                                        .getName();
                        }

                        throw new IllegalArgumentException("Illegal column index: " + col);
                    });
                };
            }
            else if ("variables".equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    final List<Entry<String, Object>> variables = new ArrayList<>(((ExecutionContext) ctx).getVariables()
                            .entrySet());
                    return new ObjectTupleVector(VARIABLES_SCHEMA, variables.size(), (row, col) ->
                    {
                        Entry<String, Object> e = variables.get(row);
                        if (col == 0)
                        {
                            return e.getKey();
                        }

                        if (e.getValue() instanceof ValueVector)
                        {
                            return ((ValueVector) e.getValue()).valueAsObject(0);
                        }

                        return e.getValue();
                    });
                };
            }
        }

        if (vector == null)
        {
            throw new RuntimeException("System table: " + table + " is not supported");
        }

        final Function<IExecutionContext, TupleVector> tupleVector = vector;
        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
            {
                return TupleIterator.singleton(tupleVector.apply(context));
            }
        };
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        int size = table.size();
        QuerySession querySession = (QuerySession) session;
        String targetCatalogAlias;
        Catalog catalog;
        if (size == 1)
        {
            targetCatalogAlias = catalogAlias;
            catalog = querySession.getCatalogRegistry()
                    .getSystemCatalog();
        }
        else
        {
            // Strip sys from qname
            targetCatalogAlias = table.getFirst();
            catalog = querySession.getCatalogRegistry()
                    .getCatalog(targetCatalogAlias);
            if (catalog == null)
            {
                // If there is no registerd catalog then redirect the whole qualifier to
                // system catalog
                targetCatalogAlias = catalogAlias;
                catalog = querySession.getCatalogRegistry()
                        .getSystemCatalog();
            }
            else
            {
                table = table.extract(1);
            }
        }

        DatasourceData newData = new DatasourceData(data.getNodeId(), data.getPredicates(), data.getSortItems(), data.getProjection());
        return catalog.getSystemTableDataSource(querySession, targetCatalogAlias, table, newData);
    }

    @Override
    public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        int size = table.size();
        QuerySession querySession = (QuerySession) session;
        String targetCatalogAlias;
        Catalog catalog;
        if (size == 1)
        {
            targetCatalogAlias = catalogAlias;
            catalog = querySession.getCatalogRegistry()
                    .getSystemCatalog();
        }
        else
        {
            // Strip sys from qname
            targetCatalogAlias = table.getFirst();
            catalog = querySession.getCatalogRegistry()
                    .getCatalog(targetCatalogAlias);
            if (catalog == null)
            {
                // If there is no registerd catalog then redirect the whole qualifier to
                // system catalog
                targetCatalogAlias = catalogAlias;
                catalog = querySession.getCatalogRegistry()
                        .getSystemCatalog();
            }
            else
            {
                table = table.extract(1);
            }
        }

        return catalog.getSystemTableSchema(querySession, targetCatalogAlias, table);
    }
}
