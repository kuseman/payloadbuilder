package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.SelectIntoData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.cache.Cache;
import se.kuseman.payloadbuilder.core.cache.CacheProvider;
import se.kuseman.payloadbuilder.core.cache.CacheType;
import se.kuseman.payloadbuilder.core.catalog.system.AMatchFunction.MatchType;
import se.kuseman.payloadbuilder.core.catalog.system.TrimFunction.Type;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Built in system catalog */
public class SystemCatalog extends Catalog
{
    public static final String NAME = "System";

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
            Column.of("indices",       ResolvedType.of(se.kuseman.payloadbuilder.api.catalog.Column.Type.String)),
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
        catalog.registerFunction(new LowerUpperFunction(true));
        catalog.registerFunction(new LowerUpperFunction(false));
        catalog.registerFunction(new SubstringFunction());
        catalog.registerFunction(new TrimFunction(Type.BOTH));
        catalog.registerFunction(new TrimFunction(Type.LEFT));
        catalog.registerFunction(new TrimFunction(Type.RIGHT));
        catalog.registerFunction(new PadFunction(true));
        catalog.registerFunction(new PadFunction(false));
        catalog.registerFunction(new ReplaceFunction());
        catalog.registerFunction(new LengthFunction());
        catalog.registerFunction(new FormatFunction());
        catalog.registerFunction(new ConcatFunction());
        catalog.registerFunction(new CharFunction());
        catalog.registerFunction(new LeftRightFunction(true));
        catalog.registerFunction(new LeftRightFunction(false));
        catalog.registerFunction(new ReverseFunction());
        catalog.registerFunction(new CharIndexFunction());
        catalog.registerFunction(new StringAggFunction());
        catalog.registerFunction(new Base64Function(true));
        catalog.registerFunction(new Base64Function(false));
        catalog.registerFunction(new StringSplitScalarFunction());

        // Date functions
        catalog.registerFunction(new GetDateFunction(true));
        catalog.registerFunction(new GetDateFunction(false));
        catalog.registerFunction(new UnixTimeStampFunction());
        catalog.registerFunction(new CurrentTimeZoneFunction());

        // Json functions
        catalog.registerFunction(IsJsonFunction.isJson());
        catalog.registerFunction(IsJsonFunction.isJsonObject());
        catalog.registerFunction(IsJsonFunction.isJsonArray());
        catalog.registerFunction(new JsonValueFunction());

        // Misc. functions
        catalog.registerFunction(new HashFunction());
        catalog.registerFunction(new DistinctFunction());
        catalog.registerFunction(new IsNullFunction());
        catalog.registerFunction(new IsBlankFunction());
        catalog.registerFunction(new CoalesceFunction());
        catalog.registerFunction(new RegexpLikeFunction());
        catalog.registerFunction(new RegexpMatchFunction());
        catalog.registerFunction(new TypeOfFunction());
        catalog.registerFunction(new ContainsFunction());
        catalog.registerFunction(new RandomInt());
        catalog.registerFunction(new ArrayFunction());
        catalog.registerFunction(new ToArrayFunction());
        catalog.registerFunction(new ToTableFunction());
        catalog.registerFunction(new LeastGreatestFunction(false));
        catalog.registerFunction(new LeastGreatestFunction(true));
        catalog.registerFunction(new ParseDurationFunction());
        catalog.registerFunction(new ParseDataSizeFunction());

        // Mathematical functions
        catalog.registerFunction(new AbsFunction());
        catalog.registerFunction(new CeilingFunction());
        catalog.registerFunction(new FloorFunction());

        // Lambda functions
        catalog.registerFunction(new FilterFunction());
        catalog.registerFunction(new MapFunction());
        catalog.registerFunction(new FlatMapFunction());
        catalog.registerFunction(new AMatchFunction(MatchType.ANY));
        catalog.registerFunction(new AMatchFunction(MatchType.ALL));
        catalog.registerFunction(new AMatchFunction(MatchType.NONE));

        // Table functions
        catalog.registerFunction(new RangeFunction());
        catalog.registerFunction(new StringSplitTableFunction());
        catalog.registerFunction(new OpenJsonFunction());
        catalog.registerFunction(new OpenCsvFunction());
        catalog.registerFunction(new OpenXmlFunction());

        // Aggregate functions
        catalog.registerFunction(new AggregateAvgFunction());
        catalog.registerFunction(new AggregateMinMaxFunction(true));
        catalog.registerFunction(new AggregateMinMaxFunction(false));
        catalog.registerFunction(new AggregateCountFunction());
        catalog.registerFunction(new AggregateSumFunction());
        catalog.registerFunction(new AggregateObjectArrayFunction());
        catalog.registerFunction(new ObjectFunction());

        // Operator functions
        catalog.registerFunction(new OperatorArrayFunction());
        catalog.registerFunction(new OperatorObjectArrayFunction());
        catalog.registerFunction(new OperatorObjectFunction());

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
                vector = ctx -> getFunctionsTupleVector(FUNCTIONS_SCHEMA);
            }
            else if (SYS_TABLES.equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    final List<Entry<QualifiedName, TemporaryTable>> tables = new ArrayList<>(((QuerySession) ctx.getSession()).getTemporaryTables());
                    return new ObjectTupleVector(TABLES_SCHEMA, tables.size(), (row, col) ->
                    {
                        Entry<QualifiedName, TemporaryTable> e = tables.get(row);
                        if (col == 0)
                        {
                            return e.getKey();
                        }
                        else if (col == 1)
                        {
                            return e.getValue()
                                    .getTupleVector()
                                    .getSchema()
                                    .getColumns()
                                    .stream()
                                    .map(c -> c.getName() + " (" + c.getType() + ")")
                                    .collect(joining(", "));
                        }
                        else if (col == 2)
                        {
                            return e.getValue()
                                    .getIndices()
                                    .stream()
                                    .map(i -> i.getColumnsType() + ": " + i.getColumns())
                                    .collect(joining(", "));
                        }

                        return e.getValue()
                                .getTupleVector()
                                .getRowCount();
                    });

                };
            }
            else if ("caches".equalsIgnoreCase(type))
            {
                vector = ctx ->
                {
                    QuerySession querySession = (QuerySession) ctx.getSession();
                    List<Pair<CacheProvider, Cache>> caches = asList(querySession.getTempTableCache(), querySession.getGenericCache()).stream()
                            .flatMap(p -> p.getCaches()
                                    .stream()
                                    .map(c -> Pair.of(p, c)))
                            .collect(toList());
                    return new ObjectTupleVector(CACHES_SCHEMA, caches.size(), (row, col) ->
                    {
                        Pair<CacheProvider, Cache> pair = caches.get(row);
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

                    final List<Triple<CacheProvider, Cache, Cache.CacheEntry>> cacheKeys = new ArrayList<>();
                    for (CacheProvider provider : asList(querySession.getTempTableCache(), querySession.getGenericCache()))
                    {
                        for (Cache cache : provider.getCaches())
                        {
                            for (Cache.CacheEntry entry : cache.getCacheEntries())
                            {
                                cacheKeys.add(Triple.of(provider, cache, entry));
                            }
                        }
                    }

                    return new ObjectTupleVector(CACHE_KEYS_SCHEMA, cacheKeys.size(), (row, col) ->
                    {
                        Triple<CacheProvider, Cache, Cache.CacheEntry> t = cacheKeys.get(row);
                        // CSOFF
                        switch (col)
                        // CSON
                        {
                            case 0:
                                return t.getMiddle()
                                        .getName();
                            case 1:
                                return t.getRight()
                                        .getKey();
                            case 2:
                                return t.getRight()
                                        .getExpireTime();
                            case 3:
                                return CacheType.from(t.getLeft());
                            case 4:
                                return t.getLeft()
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
                    final List<Entry<String, ValueVector>> variables = new ArrayList<>(((ExecutionContext) ctx).getVariables()
                            .entrySet());
                    return new ObjectTupleVector(VARIABLES_SCHEMA, variables.size(), (row, col) ->
                    {
                        Entry<String, ValueVector> e = variables.get(row);
                        if (col == 0)
                        {
                            return e.getKey();
                        }
                        return e.getValue()
                                .valueAsObject(0);
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
            public TupleIterator execute(IExecutionContext context)
            {
                return TupleIterator.singleton(tupleVector.apply(context));
            }
        };
    }

    @Override
    public IDatasink getSelectIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, SelectIntoData data)
    {
        if (!"#".equalsIgnoreCase(table.getFirst()))
        {
            throw new CompileException("Can only insert into temp tables (prefixed with '#'). Table: " + table);
        }

        return new SelectIntoTempTableSink(table, data.getOptions(), true);
    }

    @Override
    public void dropTable(IQuerySession session, String catalogAlias, QualifiedName qname, boolean lenient)
    {
        ((QuerySession) session).dropTemporaryTable(qname, lenient);
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        if ("#".equals(table.getFirst()))
        {
            table = table.extract(1)
                    .toLowerCase();
            return new TemporaryTableDataSource(table, null);
        }

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

        DatasourceData newData = new DatasourceData(data.getNodeId(), data.getPredicates(), data.getSortItems(), data.getProjection(), data.getOptions());
        return catalog.getSystemTableDataSource(querySession, targetCatalogAlias, table, newData);
    }

    @Override
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        if ("#".equals(seekPredicate.getIndex()
                .getTable()
                .getFirst()))
        {
            QualifiedName table = seekPredicate.getIndex()
                    .getTable()
                    .extract(1)
                    .toLowerCase();
            return new TemporaryTableDataSource(table, seekPredicate);
        }
        throw new CompileException("Index: " + seekPredicate.getIndex() + " is not supported");
    }

    @Override
    public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
    {
        // Temporary table
        if ("#".equals(table.getFirst()))
        {
            table = table.extract(1)
                    .toLowerCase();
            TableSchema tableSchema = ((QuerySession) context.getSession()).getTemporaryTableSchema(table);
            if (tableSchema == null)
            {
                throw new QueryException("No temporary table found with name #" + table);
            }

            if (SchemaUtils.isAsterisk(tableSchema.getSchema()))
            {
                return new TableSchema(Schema.EMPTY, tableSchema.getIndices());
            }

            return tableSchema;
        }

        int size = table.size();
        QuerySession querySession = (QuerySession) context.getSession();
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
