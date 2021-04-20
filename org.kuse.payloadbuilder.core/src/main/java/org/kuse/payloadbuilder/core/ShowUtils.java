package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.cache.CacheProvider;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.Column;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.ShowStatement;

/** Utils for Show statements */
class ShowUtils
{
    private static final TableAlias SHOW_VARIABLES_ALIAS = TableAliasBuilder
            .of(-1, TableAlias.Type.TABLE, QualifiedName.of("variables"), "v")
            .tableMeta(new TableMeta(asList(
                    new TableMeta.Column("Name", DataType.ANY),
                    new TableMeta.Column("Value", DataType.ANY))))
            .build();
    private static final TableAlias SHOW_TABLES_ALIAS = TableAliasBuilder
            .of(-1, TableAlias.Type.TABLE, QualifiedName.of("tables"), "t")
            .tableMeta(new TableMeta(asList(
                    new TableMeta.Column("Name", DataType.ANY))))
            .build();
    private static final TableAlias SHOW_FUNCTIONS_ALIAS = TableAliasBuilder
            .of(-1, TableAlias.Type.TABLE, QualifiedName.of("functions"), "f")
            .tableMeta(new TableMeta(asList(
                    new TableMeta.Column("Name", DataType.ANY),
                    new TableMeta.Column("Type", DataType.ANY),
                    new TableMeta.Column("Description", DataType.ANY))))
            .build();
    private static final TableAlias SHOW_CACHES_ALIAS = TableAliasBuilder
            .of(-1, TableAlias.Type.TABLE, QualifiedName.of("caches"), "f")
            .tableMeta(new TableMeta(asList(
                    new TableMeta.Column("Name", DataType.ANY),
                    new TableMeta.Column("Size", DataType.ANY),
                    new TableMeta.Column("Hits", DataType.ANY),
                    new TableMeta.Column("Hit ratio", DataType.ANY),
                    new TableMeta.Column("Misses", DataType.ANY),
                    new TableMeta.Column("Miss ratio", DataType.ANY),
                    new TableMeta.Column("Type", DataType.ANY),
                    new TableMeta.Column("Provider", DataType.ANY))))
            .build();

    private ShowUtils()
    {
    }

    /** Get show select */
    //CSOFF
    static Pair<Operator, Projection> getShowSelect(ExecutionContext context, ShowStatement statement)
    //CSON
    {
        Operator operator = null;
        MutableInt pos = new MutableInt();
        List<TableMeta.Column> columns = null;
        QuerySession session = context.getSession();
        CatalogRegistry registry = session.getCatalogRegistry();

        if (statement.getType() == ShowStatement.Type.VARIABLES)
        {
            Map<String, Object> variables = context.getVariables();
            columns = SHOW_VARIABLES_ALIAS.getTableMeta().getColumns();
            operator = new Operator()
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(variables
                            .entrySet()
                            .stream()
                            .map(e -> (Tuple) Row.of(SHOW_VARIABLES_ALIAS, pos.incrementAndGet(), new Object[] {e.getKey(), e.getValue()}))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else if (statement.getType() == ShowStatement.Type.TABLES)
        {
            String alias = defaultIfBlank(statement.getCatalog(), registry.getDefaultCatalogAlias());
            if (isBlank(alias))
            {
                throw new ParseException("No catalog alias provided.", statement.getToken());
            }
            Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
            if (catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + alias, statement.getToken());
            }

            List<String> tables = new ArrayList<>();
            tables.addAll(session.getTemporaryTableNames().stream().map(t -> "#" + t.toString()).collect(toList()));
            tables.addAll(catalog.getTables(session, alias));
            columns = SHOW_TABLES_ALIAS.getTableMeta().getColumns();
            operator = new Operator()
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(tables
                            .stream()
                            .map(table -> (Tuple) Row.of(SHOW_TABLES_ALIAS, pos.incrementAndGet(), new Object[] {table}))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else if (statement.getType() == ShowStatement.Type.FUNCTIONS)
        {
            String alias = defaultIfBlank(statement.getCatalog(), registry.getDefaultCatalogAlias());
            Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
            if (!isBlank(statement.getCatalog()) && catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + statement.getCatalog(), statement.getToken());
            }

            Catalog builtIn = session.getCatalogRegistry().getBuiltin();
            Collection<FunctionInfo> functions = catalog != null ? catalog.getFunctions() : emptyList();
            columns = SHOW_FUNCTIONS_ALIAS.getTableMeta().getColumns();
            //CSOFF
            operator = new Operator()
            //CSON
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(Stream.concat(
                            functions
                                    .stream()
                                    .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                    .map(function -> (Tuple) Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {function.getName(), function.getType(), function.getDescription()})),
                            Stream.concat(
                                    functions.size() > 0
                                        ? Stream.of(Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {"-- Built in --", "", ""}))
                                        : Stream.empty(),
                                    builtIn.getFunctions()
                                            .stream()
                                            .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                            .map(function -> Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {function.getName(), function.getType(), function.getDescription()}))))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else if (statement.getType() == ShowStatement.Type.CACHES)
        {
            columns = SHOW_CACHES_ALIAS.getTableMeta().getColumns();
            final List<CacheProvider> providers = asList(
                    session.getBatchCacheProvider(),
                    session.getTempTableCacheProvider(),
                    session.getCustomCacheProvider());

            //CSOFF
            operator = new Operator()
            //CSON
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(
                            providers
                                    .stream()
                                    .flatMap(p -> p.getCaches().stream().map(c -> Pair.of(p, c)))
                                    .map(p -> (Tuple) Row.of(
                                            SHOW_CACHES_ALIAS,
                                            pos.incrementAndGet(),
                                            new Object[] {
                                                    p.getValue().getName().toString(),
                                                    p.getValue().getSize(),
                                                    p.getValue().getCacheHits(),
                                                    (float) p.getValue().getCacheHits() / (p.getValue().getCacheHits() + p.getValue().getCacheMisses()),
                                                    p.getValue().getCacheMisses(),
                                                    (float) p.getValue().getCacheMisses() / (p.getValue().getCacheHits() + p.getValue().getCacheMisses()),
                                                    p.getKey().getType().toString(),
                                                    p.getKey().getName()
                                            }))
                                    .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }

        List<String> colList = columns.stream().map(Column::getName).collect(toList());
        return Pair.of(operator, DescribeUtils.getIndexProjection(colList));
    }
}
