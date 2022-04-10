package org.kuse.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.upperCase;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.cache.CacheProvider;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.system.MatchFunction.MatchType;
import org.kuse.payloadbuilder.core.catalog.system.TrimFunction.Type;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Built in system catalog */
public class SystemCatalog extends Catalog
{
    public static final String NAME = "System";
    public static final String ALIAS = "sys";

    SystemCatalog()
    {
        super(NAME);
    }

    /** Get instance of system catalog */
    public static Catalog get()
    {
        SystemCatalog catalog = new SystemCatalog();

        // Scalar functions
        catalog.registerFunction(new ConcatFunction(catalog));
        catalog.registerFunction(new HashFunction(catalog));
        catalog.registerFunction(new FilterFunction(catalog));
        catalog.registerFunction(new MapFunction(catalog));
        catalog.registerFunction(new SumFunction(catalog));
        catalog.registerFunction(new FlatMapFunction(catalog));
        catalog.registerFunction(new MatchFunction(catalog, MatchType.ANY));
        catalog.registerFunction(new MatchFunction(catalog, MatchType.ALL));
        catalog.registerFunction(new MatchFunction(catalog, MatchType.NONE));
        catalog.registerFunction(new RandomInt(catalog));
        catalog.registerFunction(new CountFunction(catalog));
        catalog.registerFunction(new MinFunction(catalog));
        catalog.registerFunction(new MaxFunction(catalog));
        catalog.registerFunction(new DistinctFunction(catalog));
        catalog.registerFunction(new IsNullFunction(catalog));
        catalog.registerFunction(new IsBlankFunction(catalog));
        catalog.registerFunction(new CoalesceFunction(catalog));
        catalog.registerFunction(new JsonValueFunction(catalog));
        catalog.registerFunction(new CastFunction(catalog, "cast"));
        catalog.registerFunction(new CastFunction(catalog, "convert"));
        catalog.registerFunction(new RegexpLikeFunction(catalog));
        catalog.registerFunction(new LowerUpperFunction(catalog, true));
        catalog.registerFunction(new LowerUpperFunction(catalog, false));
        catalog.registerFunction(new SubstringFunction(catalog));
        catalog.registerFunction(new TrimFunction(catalog, Type.BOTH));
        catalog.registerFunction(new TrimFunction(catalog, Type.LEFT));
        catalog.registerFunction(new TrimFunction(catalog, Type.RIGHT));
        catalog.registerFunction(new PadFunction(catalog, true));
        catalog.registerFunction(new PadFunction(catalog, false));
        catalog.registerFunction(new LengthFunction(catalog));
        catalog.registerFunction(new ReplaceFunction(catalog));
        catalog.registerFunction(new TypeOfFunction(catalog));
        catalog.registerFunction(new GetDateFunction(catalog, true));
        catalog.registerFunction(new GetDateFunction(catalog, false));
        catalog.registerFunction(new DatePartFunction(catalog));
        catalog.registerFunction(new DateAddFunction(catalog));
        catalog.registerFunction(new UnixTimeStampFunction(catalog));
        catalog.registerFunction(new ListOfFunction(catalog));
        catalog.registerFunction(new UnionFunction(catalog, true));
        catalog.registerFunction(new ContainsFunction(catalog));

        // Table functions
        catalog.registerFunction(new RangeFunction(catalog));
        catalog.registerFunction(new OpenMapCollectionFunction(catalog));
        catalog.registerFunction(new OpenRowsFunction(catalog));

        return catalog;
    }

    @Override
    public Operator getSystemOperator(OperatorData data)
    {
        TableAlias alias = data.getTableAlias();
        QualifiedName table = alias.getTable();

        if (table.size() == 1)
        {
            String type = table.getLast();
            if ("catalogs".equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getCatalogsIterator(ctx.getSession(), alias));
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return getFunctionsOperator(data.getNodeId(), alias);
            }
            else if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getTempTablesIterator(ctx.getSession(), alias));
            }
            else if ("caches".equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getCachesIterator(ctx.getSession(), alias));
            }
            else if ("cachekeys".equalsIgnoreCase(type))
            {
                Expression typeExpression = data.extractPredicate("type");
                Expression nameExpression = data.extractPredicate("name");

                return systemOperator(data.getNodeId(), table.toDotDelimited(), ctx ->
                {
                    String typeName = typeExpression != null
                        ? String.valueOf(typeExpression.eval(ctx))
                        : null;
                    String cacheName = nameExpression != null
                        ? String.valueOf(nameExpression.eval(ctx))
                        : null;

                    CacheProvider.Type providerType = typeName != null
                        ? CacheProvider.Type.valueOf(upperCase(typeName))
                        : null;

                    return getCacheKeysIterator(ctx.getSession(), alias, providerType, cacheName);
                });
            }
            else if ("variables".equalsIgnoreCase(type))
            {
                return systemOperator(data.getNodeId(), type, ctx -> getVariablesIterator(ctx, alias));
            }
        }

        throw new RuntimeException("System table: " + table + " is not supported");
    }

    @Override
    public Operator getScanOperator(OperatorData data)
    {
        final TableAlias alias = data.getTableAlias();
        QualifiedName table = alias.getTable();
        int size = table.size();

        /*
         * 1-part => system catalog
         *      catalogs
         *      caches
         * 2-part => redirect to catalog system scan operator
         *      sys.tables
         *      sys.columns
         *      sys.indices
         *      sys.functions
         *      sys.<custom-table>
         */

        String catalogAlias;
        Catalog catalog;
        if (size == 1)
        {
            catalogAlias = "";
            catalog = data.getSession().getCatalogRegistry().getSystemCatalog();
        }
        else
        {
            // Strip sys from qname
            catalogAlias = table.getFirst();
            catalog = data.getSession().getCatalogRegistry().getCatalog(catalogAlias);
            if (catalog == null)
            {
                // If there is no registerd catalog then redirect the whole qualifier to
                // system catalog
                catalog = data.getSession().getCatalogRegistry().getSystemCatalog();
            }
            else
            {
                table = table.extract(1);
            }
        }

        TableAlias newAlias = TableAliasBuilder.of(
                alias.getTupleOrdinal(),
                alias.getType(),
                table,
                alias.getAlias())
                .build();

        OperatorData newData = new OperatorData(
                data.getSession(),
                data.getNodeId(),
                catalogAlias,
                newAlias,
                data.getPredicatePairs(),
                data.getSortItems(),
                data.getTableOptions());

        return catalog.getSystemOperator(newData);
    }

    private TupleIterator getCatalogsIterator(QuerySession session, TableAlias tableAlias)
    {
        String[] columns = new String[] {"alias", "name"};
        return TupleIterator.wrap(session
                .getCatalogRegistry()
                .getCatalogs()
                .stream()
                .map(e -> (Tuple) Row.of(tableAlias, columns, new Object[] {e.getKey(), e.getValue().getName()}))
                .iterator());
    }

    private TupleIterator getTempTablesIterator(QuerySession session, TableAlias tableAlias)
    {
        String[] columns = new String[] {SYS_TABLES_NAME, "rows"};
        return TupleIterator.wrap(session
                .getTemporaryTables()
                .stream()
                .map(e -> (Tuple) Row.of(tableAlias, columns, new Object[] {e, e.getRows().size()}))
                .iterator());
    }

    private TupleIterator getCachesIterator(QuerySession session, TableAlias tableAlias)
    {
        final List<CacheProvider> providers = asList(
                session.getBatchCacheProvider(),
                session.getTempTableCacheProvider(),
                session.getCustomCacheProvider());

        String[] columns = new String[] {"name", "size", "hits", "hit_ratio", "misses", "miss_ratio", "type", "provider"};
        return TupleIterator.wrap(providers
                .stream()
                .flatMap(p -> p.getCaches().stream().map(c -> Pair.of(p, c)))
                .map(p -> (Tuple) Row.of(tableAlias, columns, new Object[] {
                        p.getValue().getName(),
                        p.getValue().getSize(),
                        p.getValue().getCacheHits(),
                        (float) p.getValue().getCacheHits() / (p.getValue().getCacheHits() + p.getValue().getCacheMisses()),
                        p.getValue().getCacheMisses(),
                        (float) p.getValue().getCacheMisses() / (p.getValue().getCacheHits() + p.getValue().getCacheMisses()),
                        p.getKey().getType(),
                        p.getKey().getName()
                }))
                .iterator());
    }

    private TupleIterator getCacheKeysIterator(QuerySession session, TableAlias tableAlias, CacheProvider.Type type, String cacheName)
    {
        List<CacheProvider> providers;

        if (type != null)
        {
            providers = singletonList(session.getCacheProvider(type));
        }
        else
        {
            providers = asList(
                    session.getBatchCacheProvider(),
                    session.getTempTableCacheProvider(),
                    session.getCustomCacheProvider());
        }

        String[] columns = new String[] {"name", "key", "expire_time", "type", "provider"};
        return TupleIterator.wrap(providers
                .stream()
                .flatMap(p -> p.getCaches().stream().map(c -> Pair.of(p, c)))
                .filter(p -> cacheName == null
                    // Try to filter on both dot delimited and string representation
                    || cacheName.equalsIgnoreCase(p.getValue().getName().toDotDelimited())
                    || cacheName.equalsIgnoreCase(p.getValue().getName().toString()))
                .flatMap(p -> p.getKey().getCacheEntries(p.getValue().getName()).stream().map(ce -> Pair.of(p, ce)))
                .map(p -> (Tuple) Row.of(tableAlias, columns, new Object[] {
                        p.getKey().getValue().getName(),
                        p.getValue().getKey(),
                        p.getValue().getExpireTime(),
                        p.getKey().getKey().getType(),
                        p.getKey().getKey().getName()
                }))
                .iterator());
    }

    private TupleIterator getVariablesIterator(ExecutionContext ctx, TableAlias tableAlias)
    {
        String[] columns = new String[] {"name", "value"};
        return TupleIterator.wrap(ctx.getVariables()
                .entrySet()
                .stream()
                .map(e -> (Tuple) Row.of(tableAlias, columns, new Object[] {e.getKey(), e.getValue()}))
                .iterator());
    }
}
