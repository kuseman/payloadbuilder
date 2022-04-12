package se.kuseman.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.TableMeta.Column;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Catalog.OperatorData;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.AsteriskProjection;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.operator.Projection;
import se.kuseman.payloadbuilder.core.operator.RootProjection;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.parser.ShowStatement;

/** Utils for Show statements */
class ShowUtils
{
    private static final TableAlias SHOW_FUNCTIONS_ALIAS = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("functions"), "f")
            .tableMeta(new TableMeta(asList(new TableMeta.Column("Name", DataType.ANY), new TableMeta.Column("Type", DataType.ANY), new TableMeta.Column("Description", DataType.ANY))))
            .build();

    private ShowUtils()
    {
    }

    /** Get show select */
    // CSOFF
    static Pair<Operator, Projection> getShowSelect(ExecutionContext context, ShowStatement statement)
    // CSON
    {
        Operator operator = null;
        Projection projection = null;
        List<TableMeta.Column> columns = null;
        QuerySession session = context.getSession();

        if (statement.getType() == ShowStatement.Type.VARIABLES)
        {
            TableAlias tableAlias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("variables"), "t")
                    .build();

            OperatorData opData = new OperatorData(session, 0, "", tableAlias, emptyList(), emptyList());

            operator = session.getCatalogRegistry()
                    .getSystemCatalog()
                    .getSystemOperator(opData);
            projection = new RootProjection(asList(""), asList(new AsteriskProjection(new int[] { 0 })));
        }
        else if (statement.getType() == ShowStatement.Type.TABLES)
        {
            String alias = defaultIfBlank(statement.getCatalog(), session.getDefaultCatalogAlias());
            if (isBlank(alias))
            {
                throw new ParseException("No catalog alias provided.", statement.getToken());
            }
            Catalog catalog = session.getCatalogRegistry()
                    .getCatalog(alias);
            if (catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + alias, statement.getToken());
            }

            TableAlias tableAlias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("tables"), "t")
                    .build();

            OperatorData opData = new OperatorData(session, 0, alias, tableAlias, emptyList(), emptyList());

            operator = catalog.getSystemOperator(opData);
            projection = new RootProjection(asList(""), asList(new AsteriskProjection(new int[] { 0 })));
        }
        else if (statement.getType() == ShowStatement.Type.FUNCTIONS)
        {
            String alias = defaultIfBlank(statement.getCatalog(), session.getDefaultCatalogAlias());
            Catalog catalog = session.getCatalogRegistry()
                    .getCatalog(alias);
            if (!isBlank(statement.getCatalog())
                    && catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + statement.getCatalog(), statement.getToken());
            }

            Catalog system = session.getCatalogRegistry()
                    .getSystemCatalog();
            Collection<FunctionInfo> functions = catalog != null ? catalog.getFunctions()
                    : emptyList();
            columns = SHOW_FUNCTIONS_ALIAS.getTableMeta()
                    .getColumns();
            // CSOFF
            operator = new Operator()
            // CSON
            {
                @Override
                public TupleIterator open(IExecutionContext context)
                {
                    return TupleIterator.wrap(Stream.concat(functions.stream()
                            .sorted((a, b) -> a.getName()
                                    .compareToIgnoreCase(b.getName()))
                            .map(function -> (Tuple) Row.of(SHOW_FUNCTIONS_ALIAS, new Object[] { function.getName(), function.getType(), function.getDescription() })),
                            Stream.concat(functions.size() > 0 ? Stream.of(Row.of(SHOW_FUNCTIONS_ALIAS, new Object[] { "-- Built in --", "", "" }))
                                    : Stream.empty(),
                                    system.getFunctions()
                                            .stream()
                                            .sorted((a, b) -> a.getName()
                                                    .compareToIgnoreCase(b.getName()))
                                            .map(function -> Row.of(SHOW_FUNCTIONS_ALIAS, new Object[] { function.getName(), function.getType(), function.getDescription() }))))
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
            TableAlias tableAlias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("caches"), "t")
                    .build();

            OperatorData opData = new OperatorData(session, 0, "", tableAlias, emptyList(), emptyList());

            operator = session.getCatalogRegistry()
                    .getSystemCatalog()
                    .getSystemOperator(opData);
            projection = new RootProjection(asList(""), asList(new AsteriskProjection(new int[] { 0 })));
        }

        if (projection == null)
        {
            List<String> colList = columns.stream()
                    .map(Column::getName)
                    .collect(toList());
            projection = DescribeUtils.getIndexProjection(colList);
        }

        return Pair.of(operator, projection);
    }
}
