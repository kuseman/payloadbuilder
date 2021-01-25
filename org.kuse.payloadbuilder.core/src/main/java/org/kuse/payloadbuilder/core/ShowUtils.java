package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.ShowStatement;

/** Utils for show statement */
class ShowUtils
{
    private static final TableAlias SHOW_VARIABLES_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("variables"), "v").columns(new String[] {"Name", "Value"}).build();
    private static final TableAlias SHOW_TABLES_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tables"), "t").columns(new String[] {"Name"}).build();
    private static final TableAlias SHOW_FUNCTIONS_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("functions"), "f")
            .columns(new String[] {"Name", "Type", "Description"})
            .build();
    private static final TableAlias SHOW_CACHES_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("caches"), "c").columns(new String[] {"Name"}).build();
    private static final Pair<Operator, Projection> EMPTY_PAIR = Pair.of(Operator.EMPTY_OPERATOR, Projection.EMPTY_PROJECTION);

    private ShowUtils()
    {
    }

    /** Create show operator for provided statement */
    static Pair<Operator, Projection> createShowOperator(ExecutionContext context, ShowStatement statement)
    {
        switch (statement.getType())
        {
            case CACHES:
                return showCaches(context);
            case FUNCTIONS:
                return showFunctions(context, statement);
            case TABLES:
                return showTables(context, statement);
            case VARIABLES:
                return showVariables(context);
            default:
                throw new IllegalArgumentException("Unknown show type: " + statement.getType());
        }
    }

    private static Pair<Operator, Projection> showVariables(ExecutionContext context)
    {
        Map<String, Object> variables = context.getVariables();
        String[] columns = SHOW_VARIABLES_ALIAS.getColumns();
        Operator operator = new Operator()
        {
            private int pos;

            @Override
            public RowIterator open(ExecutionContext context)
            {
                return RowIterator.wrap(variables
                        .entrySet()
                        .stream()
                        .map(e -> (Tuple) Row.of(SHOW_VARIABLES_ALIAS, pos++, new Object[] {e.getKey(), e.getValue()}))
                        .iterator());
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        return Pair.of(operator, DescribeUtils.getIndexProjection(asList(columns)));
    }

    private static Pair<Operator, Projection> showTables(ExecutionContext context, ShowStatement statement)
    {
        QuerySession session = context.getSession();
        String alias = defaultIfBlank(statement.getCatalog(), session.getCatalogRegistry().getDefaultCatalogAlias());
        if (isBlank(alias))
        {
            throw new ParseException("No catalog alias provided.", statement.getToken());
        }
        Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
        if (catalog == null)
        {
            throw new ParseException("No catalog found with alias: " + alias, statement.getToken());
        }

        List<String> tables = catalog.getTables(session, alias);
        String[] columns = SHOW_TABLES_ALIAS.getColumns();
        Operator operator = new Operator()
        {
            private int pos;

            @Override
            public RowIterator open(ExecutionContext context)
            {
                return RowIterator.wrap(tables
                        .stream()
                        .map(table -> (Tuple) Row.of(SHOW_TABLES_ALIAS, pos++, new Object[] {table}))
                        .iterator());
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        return Pair.of(operator, DescribeUtils.getIndexProjection(asList(columns)));
    }

    private static Pair<Operator, Projection> showFunctions(ExecutionContext context, ShowStatement statement)
    {
        String alias = defaultIfBlank(statement.getCatalog(), context.getSession().getCatalogRegistry().getDefaultCatalogAlias());
        QuerySession session = context.getSession();
        Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
        if (!isBlank(statement.getCatalog()) && catalog == null)
        {
            throw new ParseException("No catalog found with alias: " + statement.getCatalog(), statement.getToken());
        }

        Catalog builtIn = session.getCatalogRegistry().getBuiltin();
        Collection<FunctionInfo> functions = catalog != null ? catalog.getFunctions() : emptyList();
        String[] columns = SHOW_FUNCTIONS_ALIAS.getColumns();
        //CSOFF
        Operator operator = new Operator()
        //CSON
        {
            private int pos;

            @Override
            public RowIterator open(ExecutionContext context)
            {
                return RowIterator.wrap(Stream.concat(
                        functions
                                .stream()
                                .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                .map(function -> (Tuple) Row.of(SHOW_FUNCTIONS_ALIAS, pos++, new Object[] {function.getName(), function.getType(), function.getDescription()})),
                        Stream.concat(
                                functions.size() > 0
                                    ? Stream.of(Row.of(SHOW_FUNCTIONS_ALIAS, pos++, new Object[] {"-- Built in --", "", ""}))
                                    : Stream.empty(),
                                builtIn.getFunctions()
                                        .stream()
                                        .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                        .map(function -> Row.of(SHOW_FUNCTIONS_ALIAS, pos++, new Object[] {function.getName(), function.getType(), function.getDescription()}))))
                        .iterator());
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };
        return Pair.of(operator, DescribeUtils.getIndexProjection(asList(columns)));
    }

    private static Pair<Operator, Projection> showCaches(ExecutionContext context)
    {
        TupleCacheProvider cacheProvider = context.getSession().getTupleCacheProvider();
        if (cacheProvider == null)
        {
            return EMPTY_PAIR;
        }

        final Map<String, Map<String, Object>> cacheProviderDescription = cacheProvider.describe();
        if (MapUtils.isEmpty(cacheProviderDescription))
        {
            return EMPTY_PAIR;
        }

        List<String> columnsList = new ArrayList<>();
        columnsList.add("Name");
        cacheProviderDescription.values().stream().forEach(m ->
        {
            for (String col : m.keySet())
            {
                if (!columnsList.contains(col))
                {
                    columnsList.add(col);
                }
            }
        });
        int size = columnsList.size();
        String[] columns = columnsList.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        //CSOFF
        Operator operator = new Operator()
        //CSON
        {
            private int pos;

            @Override
            public RowIterator open(ExecutionContext context)
            {
                return RowIterator.wrap(cacheProviderDescription
                        .entrySet()
                        .stream()
                        .sorted((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.getKey(), b.getKey()))
                        .map(e ->
                        {
                            List<Object> values = new ArrayList<>(1 + e.getValue().size());
                            values.add(e.getKey());
                            for (int i = 1; i < size; i++)
                            {
                                values.add(e.getValue().get(columns[i]));
                            }
                            return (Tuple) Row.of(SHOW_CACHES_ALIAS, pos++, columns, values.toArray());
                        })
                        .iterator());
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        return Pair.of(operator, DescribeUtils.getIndexProjection(columnsList));
    }
}
