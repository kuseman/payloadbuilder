package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.InExpression;
import se.kuseman.payloadbuilder.core.expression.LikeExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralArrayExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.expression.NullPredicateExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeInterceptor;
import se.kuseman.payloadbuilder.core.physicalplan.Assert;
import se.kuseman.payloadbuilder.core.physicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.physicalplan.DescribePlan;
import se.kuseman.payloadbuilder.core.physicalplan.ExpressionPredicate;
import se.kuseman.payloadbuilder.core.physicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.physicalplan.Filter;
import se.kuseman.payloadbuilder.core.physicalplan.HashAggregate;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.physicalplan.IndexSeek;
import se.kuseman.payloadbuilder.core.physicalplan.Limit;
import se.kuseman.payloadbuilder.core.physicalplan.NestedLoop;
import se.kuseman.payloadbuilder.core.physicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.physicalplan.Projection;
import se.kuseman.payloadbuilder.core.physicalplan.Sort;
import se.kuseman.payloadbuilder.core.physicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.physicalplan.TableScan;
import se.kuseman.payloadbuilder.core.planning.QueryPlanner.TemporaryTableDataSource;
import se.kuseman.payloadbuilder.core.statement.PhysicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link QueryPlanner} */
public class QueryPlannerTest extends APhysicalPlanTest
{
    @Before
    public void setup()
    {
        session.setDefaultCatalogAlias("t");
    }

    @Test
    public void test_outer_apply_non_correlated()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "outer apply ("
                + "  select * "
                + "  from tableB b "
                + ") b ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");

        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        //@formatter:off
        IPhysicalPlan expected = NestedLoop.leftJoin(
                2,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                null);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_outer_apply_correlated()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "outer apply ("
                + "  select * "
                + "  from tableB b "
                + "  where id = a.id "
                + ") b ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");

        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        //@formatter:off
        IPhysicalPlan expected = NestedLoop.leftJoin(
                3,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new Filter(
                    2,
                    new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                    new ExpressionPredicate(eq(cre("id", tableB), ocre("id", tableA)))),
                asSet(ast("id", ResolvedType.of(Type.Any), tableA)),
                null);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_cross_apply_correlated()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "cross apply ("
                + "  select * "
                + "  from tableB b "
                + "  where id = a.id "
                + ") b ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");

        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        //@formatter:off
        IPhysicalPlan expected = NestedLoop.innerJoin(
                3,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new Filter(
                    2,
                    new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                    new ExpressionPredicate(eq(cre("id", tableB), ocre("id", tableA)))),
                asSet(ast("id", ResolvedType.of(Type.Any), tableA)),
                null);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_cross_join()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "cross join tableB b "
                + "where b.active ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
            {
                if (table.toString()
                        .equalsIgnoreCase("tableB"))
                {
                    return new TableSchema(Schema.EMPTY, asList(new Index(table, asList("col"), ColumnsType.ANY_IN_ORDER)));
                }
                return super.getTableSchema(session, catalogAlias, table, options);
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");

        //@formatter:off
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        
        IPhysicalPlan expected = NestedLoop.innerJoin(
                3,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new Filter(
                    2,
                    new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                    new ExpressionPredicate(eq(cre("active", tableB), LiteralBooleanExpression.TRUE))),
                null,
                false);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_top()
    {
        //@formatter:off
        String query = ""
                + "select top 100 * "
                + "from tableA a ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));

        //@formatter:off
        IPhysicalPlan expected = new Limit(
                1,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                intLit(100));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_top_temp_table()
    {
        //@formatter:off
        String query = "select 1 col1 into #tableA "
                + "select top 100 * "
                + "from #tableA a ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(1)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");

        Schema schema = Schema.of(col("col1", ResolvedType.of(Type.Int), tableA));
        //@formatter:off
        IPhysicalPlan expected = new Limit(
                1,
                new TableScan(0, schema, tableA, "System", true, new TemporaryTableDataSource(Optional.of(schema), QualifiedName.of("tablea"), null),
                emptyList()),
                intLit(100));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_table_function()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from range(1, 100) a ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference range = new TableSourceReference(0, "", QualifiedName.of("range"), "a");

        IPhysicalPlan expected = new TableFunctionScan(0, Schema.of(col("Value", ResolvedType.of(Type.Int), range)), range, "sys", "System", SystemCatalog.get()
                .getTableFunction("range"), asList(intLit(1), intLit(100)), emptyList());

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_order_by()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "order by col DESC";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));

        //@formatter:off
        IPhysicalPlan expected = 
                new Sort(
                    1,
                    new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                    asList(sortItem(cre("col", tableA), Order.DESC, NullOrder.UNDEFINED)));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_order_by_no_push_down_when_multiple_table_sources_are_sorted()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "cross join tableB b "
                + "order by a.col DESC, b.col2";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
            {
                IDatasource ds = super.getScanDataSource(session, catalogAlias, table, data);

                if (!data.getSortItems()
                        .isEmpty())
                {
                    fail("No push down should be possible when sorting on multiple table sources");
                }

                return ds;
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));

        //@formatter:off
        IPhysicalPlan expected =
                new Sort(
                    3,
                    NestedLoop.innerJoin(
                        2,
                        new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                        new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                        null,
                        false),
                    asList(sortItem(cre("col", tableA), Order.DESC, NullOrder.UNDEFINED), sortItem(cre("col2", tableB), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_order_by_no_push_down_when_not_top_table_source_are_sorted()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "cross join tableB b "
                + "order by b.col2";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
            {
                IDatasource ds = super.getScanDataSource(session, catalogAlias, table, data);

                if (!data.getSortItems()
                        .isEmpty())
                {
                    fail("No push down should be possible when sorting on non top table sources");
                }

                return ds;
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));

        //@formatter:off
        IPhysicalPlan expected =
                new Sort(
                    3,
                    NestedLoop.innerJoin(
                        2,
                        new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                        new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                        null,
                        false),
                    asList(sortItem(cre("col2", tableB), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_order_by_with_catalog_support()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "order by col DESC";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
            {
                IDatasource ds = super.getScanDataSource(session, catalogAlias, table, data);

                // Fake consumption of sort items
                data.getSortItems()
                        .clear();

                return ds;
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));

        //@formatter:off
        IPhysicalPlan expected = new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList());
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_order_by_with_catalog_support_not_consuming_all_items()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "order by col DESC, col2";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
            {
                IDatasource ds = super.getScanDataSource(session, catalogAlias, table, data);

                // Fake consumption of one sort item
                data.getSortItems()
                        .remove(0);

                return ds;
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        try
        {
            queryStatement = StatementPlanner.plan(session, queryStatement);
            fail("Should fail with complie exception");
        }
        catch (CompileException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Sort items must be totally consumed or left as is."));
        }
    }

    @Test
    public void test_sub_query_expression_with_switched_inputs()
    {
        //@formatter:off
        String query = ""
                + "select *, ("
                + "  select col1 "
                + "  from tableB b "
                + ") values "
                + "from tableA a ";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));

        //@formatter:off
        IPhysicalPlan expected = new Projection(
                7,
                NestedLoop.leftJoin(
                    6,
                    NestedLoop.leftJoin(
                        4,
                        new ConstantScan(0),
                        Assert.maxRowCount(
                            3,
                            new Projection(
                                2,
                                new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(0), emptyList()),
                                asList(new AliasExpression(cre("col1", tableB), "__expr0")),
                                false),
                            1),
                        null),
                    new TableScan(5, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(1), emptyList()),
                    null),
                asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA)), new AliasExpression(ce("__expr0"), "values")),
                false);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_sub_query_expression_with_switched_inputs_with_over()
    {
        //@formatter:off
        String query = ""
                + "select *, ("
                + "  select col1, col2 "
                + "  from (b) b "
                + "  for object_array "
                + ") values "
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col = a.col";
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        TableSourceReference e_b = new TableSourceReference(2, "", QualifiedName.of("b"), "b");

        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        Schema e_bExpectedSchema = Schema.of(ast("b", ResolvedType.of(Type.Any), e_b));

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            col("col1", ResolvedType.of(Type.Any), e_b),
            col("col2", ResolvedType.of(Type.Any), e_b)
        );

        IPhysicalPlan expected = new Projection(
                7,
                NestedLoop.leftJoin(
                    6,
                    NestedLoop.innerJoin(
                        2,
                        new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                        new TableScan(1, expectedSchemaB, tableB, "test", false, t.scanDataSources.get(1), emptyList()),
                        new ExpressionPredicate(eq(cre("col", tableB), cre("col", tableA))),
                        "b",
                        false),
                    new OperatorFunctionScan(
                        5,
                        new Projection(
                            4,
                            new ExpressionScan(
                                3,
                                e_b,
                                e_bExpectedSchema,
                                ocre("b", tableB, ResolvedType.table(expectedSchemaB))),
                            asList(cre("col1", e_b), cre("col2", e_b)),
                            false),
                        SystemCatalog.get().getOperatorFunction("object_array"),
                        "sys",
                        Schema.of(Column.of("__expr0", ResolvedType.table(objectArraySchema)))),
                    asSet(col("b", ResolvedType.table(expectedSchemaB), tableB)),
                    null),
                asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA, tableB)), new AliasExpression(ce("__expr0", ResolvedType.table(objectArraySchema)), "values")),
                false);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .usingOverriddenEquals()
                .ignoringFieldsOfTypes(Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_indexed_join()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "left join tableB b "
                + "  on b.col = a.col "
                + "  and a.active "
                + "  and b.active ";
               
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
            {
                if (table.toString()
                        .equalsIgnoreCase("tableB"))
                {
                    return new TableSchema(Schema.EMPTY, asList(new Index(table, asList("col"), ColumnsType.ANY_IN_ORDER)));
                }
                return super.getTableSchema(session, catalogAlias, table, options);
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");

        //@formatter:off
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        
        SeekPredicate expectedSeekPredicate = new SeekPredicate(new Index(QualifiedName.of("tableB"), asList("col"), ColumnsType.ANY_IN_ORDER), asList("col"), asList(cre("col", tableA)));
        
        IPhysicalPlan expected = NestedLoop.leftJoin(
                3,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new Filter(
                    2,
                    new IndexSeek(1, expectedSchemaB, tableB, "test", false, expectedSeekPredicate, t.seekDataSources.get(0), emptyList()),
                    new ExpressionPredicate(eq(cre("active", tableB), LiteralBooleanExpression.TRUE))),
                new ExpressionPredicate(
                    and(eq(cre("col", tableB), cre("col", tableA)), eq(cre("active", tableA), LiteralBooleanExpression.TRUE))
                ),
                null,
                true);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_indexed_join_with_sub_query_with_join()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "left join ("
                + "  select b.col, count(1) > 0 active "
                + "  from tableB b "
                + "  inner join tableC c"
                + "    on c.col = b.col "
                + "  group by b.col "
                + ") b "
                + "  on b.col = a.col "
                + "  and a.active "
                + "  and b.active ";
               
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
            {
                String tbl = table.toString();
                if (tbl.equalsIgnoreCase("tableB"))
                {
                    return new TableSchema(Schema.EMPTY, asList(new Index(table, asList("col"), ColumnsType.ANY_IN_ORDER)));
                }
                else if (tbl.equalsIgnoreCase("tableC"))
                {
                    return new TableSchema(Schema.EMPTY, asList(new Index(table, asList("col"), ColumnsType.ANY_IN_ORDER)));
                }
                return super.getTableSchema(session, catalogAlias, table, options);
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        TableSourceReference tableC = new TableSourceReference(2, "", QualifiedName.of("tableC"), "c");

        //@formatter:off
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));
        Schema expectedSchemaC = Schema.of(ast("c", ResolvedType.of(Type.Any), tableC));
        
        SeekPredicate expectedSeekPredicateB = new SeekPredicate(new Index(QualifiedName.of("tableB"), asList("col"), ColumnsType.ANY_IN_ORDER), asList("col"), asList(cre("col", tableA)));
        SeekPredicate expectedSeekPredicateC = new SeekPredicate(new Index(QualifiedName.of("tableC"), asList("col"), ColumnsType.ANY_IN_ORDER), asList("col"), asList(cre("col", tableB)));
        
        IPhysicalPlan expected = NestedLoop.leftJoin(
                5,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new HashAggregate(
                    4,
                    NestedLoop.innerJoin(
                       3,
                       new IndexSeek(1, expectedSchemaB, tableB, "test", false, expectedSeekPredicateB, t.seekDataSources.get(0), emptyList()),
                       new IndexSeek(2, expectedSchemaC, tableC, "test", false, expectedSeekPredicateC, t.seekDataSources.get(1), emptyList()),
                       new ExpressionPredicate(
                               eq(cre("col", tableC), cre("col", tableB))
                       ),
                       null,
                       true),
                    asList(cre("col", tableB)),
                    asList(
                        new AggregateWrapperExpression(cre("col", tableB), true, false),
                        new AggregateWrapperExpression(new AliasExpression(
                                new ComparisonExpression(
                                    IComparisonExpression.Type.GREATER_THAN,
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))),
                                    intLit(0)),
                                "active"), true, false)
                    )
                    ),
                new ExpressionPredicate(
                    and(
                        and(
                            eq(cre("col", tableB), cre("col", tableA)),
                            eq(cre("active", tableA), LiteralBooleanExpression.TRUE)),
                        eq(ce("active", ResolvedType.of(Type.Boolean)), LiteralBooleanExpression.TRUE))
                ),
                null,
                true);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_indexed_join_multiple_columns_any_in_order()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from tableA a "
                + "inner join tableB b "
                + "  on b.col = a.col "
                + "  and b.col2 = a.col2 ";
               
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap())
        {
            @Override
            public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
            {
                if (table.toString()
                        .equalsIgnoreCase("tableB"))
                {
                    return new TableSchema(Schema.EMPTY, asList(new Index(table, asList("col", "col2"), ColumnsType.ANY_IN_ORDER)));
                }
                return super.getTableSchema(session, catalogAlias, table, options);
            }
        };
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference tableA = new TableSourceReference(0, "", QualifiedName.of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(1, "", QualifiedName.of("tableB"), "b");
        Schema expectedSchemaA = Schema.of(ast("a", ResolvedType.of(Type.Any), tableA));
        Schema expectedSchemaB = Schema.of(ast("b", ResolvedType.of(Type.Any), tableB));

        SeekPredicate expectedSeekPredicate = new SeekPredicate(new Index(QualifiedName.of("tableB"), asList("col", "col2"), ColumnsType.ANY_IN_ORDER), asList("col", "col2"),
                asList(cre("col", tableA), cre("col2", tableA)));

        //@formatter:off
        IPhysicalPlan expected = NestedLoop.innerJoin(
                2,
                new TableScan(0, expectedSchemaA, tableA, "test", false, t.scanDataSources.get(0), emptyList()),
                new IndexSeek(1, expectedSchemaB, tableB, "test", false, expectedSeekPredicate, t.seekDataSources.get(0), emptyList()),
                new ExpressionPredicate(
                    and(eq(cre("col", tableB), cre("col", tableA)), eq(cre("col2", tableB), cre("col2", tableA)))
                ),
                null,
                true);
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" "
                + "where col = 10 "         // Comparison  
                + "and col2 in (10,20) "    // In
                + "and col3 like 'string' " // Like
                + "and col4 is null "       // Null
                + "and someFunc() "         // Function call
                + "and @var.contains(col5)";// Function call
                
        //@formatter:on

        // Capture col and "" (someFunc)
        ScalarFunctionInfo someFunc = new ScalarFunctionInfo("someFunc", FunctionInfo.FunctionType.SCALAR)
        {
        };
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("col", "col5", ""))))
        {
            {
                registerFunction(someFunc);
            }
        };

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        //@formatter:off
        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        
        IPhysicalPlan expected = new Filter(
                1,
                new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList()),
                new ExpressionPredicate(
                    and(
                        and(
                            new InExpression(cre("col2", table), asList(intLit(10), intLit(20)), false),
                            new LikeExpression(cre("col3", table), new LiteralStringExpression("string"), false, null)
                        ),
                        new NullPredicateExpression(cre("col4", table), false)))
                );
        //@formatter:on

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(QualifiedName.of("col5"), IPredicate.Type.FUNCTION_CALL, asList(
                        new FunctionCallExpression(Catalog.SYSTEM_CATALOG_ALIAS, SystemCatalog.get().getScalarFunction("contains"), null, asList(
                                new VariableExpression(QualifiedName.of("var")),
                                cre("col5", table))))),
                Triple.of(null, IPredicate.Type.FUNCTION_CALL, asList(new FunctionCallExpression("t", someFunc, null, asList()))),
                Triple.of(QualifiedName.of("col"), IPredicate.Type.COMPARISION, asList(intLit(10)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_inverted_in_expression()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" "
                + "where array(10, 20) in (col2) ";    // In
        //@formatter:on

        // Capture col2
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("col2", ""))));

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        IPhysicalPlan expected = new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList());

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.UNDEFINED, asList(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10, 20)), asList(cre("col2", table)), false)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_inverted_in_expression_multiple_args()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" "
                + "where array(10, 20) in (col2, 10) ";    // In that is not pushable
        //@formatter:on

        // Capture undefined
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet(""))));

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        IPhysicalPlan expected = new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList());

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.UNDEFINED, asList(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10, 20)), asList(cre("col2", table), intLit(10)), false)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_inverted_in_expression_with_not()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" "
                + "where array(10, 20) not in (col2) ";    // In
        //@formatter:on

        // Capture col2
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("col2", ""))));

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        IPhysicalPlan expected = new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList());

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.UNDEFINED, asList(in(new LiteralArrayExpression(VectorTestUtils.vv(Type.Int, 10, 20)), asList(cre("col2", table)), true)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_2()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" t "
                + "where t.map.col1 < 10 "             // Comparison  
                + "and t.map.col2 in (10,20) "         // In
                + "and t.map.col3 not in (10,20) "     // Not In
                + "and t.map.col4 like 'string' "      // Like
                + "and t.map.col5 not like 'string' "  // Not Like
                + "and t.map.col6 is null "            // Null
                + "and t.map.col7 is not null "        // Not Null
                + "and (col8 > 10 or col9 < 20) "      // Undefined
                + "and someFunc() ";                   // Function call
                
        //@formatter:on

        // Capture all predicates
        ScalarFunctionInfo someFunc = new ScalarFunctionInfo("someFunc", FunctionInfo.FunctionType.SCALAR)
        {
        };
        TestCatalog catalog = new TestCatalog(
                ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("map.col1", "map.col2", "map.col3", "map.col4", "map.col5", "map.col6", "map.col7", ""))))
        {
            {
                registerFunction(someFunc);
            }
        };

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");

        Schema expectedSchema = Schema.of(ast("t", ResolvedType.of(Type.Any), table));
        IPhysicalPlan expected = new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList());

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.FUNCTION_CALL, asList(new FunctionCallExpression("t", someFunc, null, asList()))),
                Triple.of(null, IPredicate.Type.UNDEFINED, asList(or(gt(cre("col8", table), intLit(10)), lt(cre("col9", table), intLit(20))))),
                Triple.of(QualifiedName.of("map", "col7"), IPredicate.Type.NULL, asList(new LiteralStringExpression("NOT NULL"))),
                Triple.of(QualifiedName.of("map", "col6"), IPredicate.Type.NULL, asList(new LiteralStringExpression("NULL"))),
                Triple.of(QualifiedName.of("map", "col5"), IPredicate.Type.LIKE, asList(new LiteralStringExpression("NOT"), new LiteralStringExpression("string"))),
                Triple.of(QualifiedName.of("map", "col4"), IPredicate.Type.LIKE, asList(new LiteralStringExpression(""), new LiteralStringExpression("string"))),
                Triple.of(QualifiedName.of("map", "col3"), IPredicate.Type.IN, asList(new LiteralStringExpression("NOT"), intLit(10), intLit(20))),
                Triple.of(QualifiedName.of("map", "col2"), IPredicate.Type.IN, asList(new LiteralStringExpression(""), intLit(10), intLit(20))),
                Triple.of(QualifiedName.of("map", "col1"), IPredicate.Type.COMPARISION, asList(intLit(10)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_with_analyze()
    {
        //@formatter:off
        String query = ""
                + "analyze select * "
                + "from \"table\" "
                + "where col = 10 "         // Comparison  
                + "and col2 in (10,20) "    // In
                + "and col3 like 'string' " // Like
                + "and col4 is null "       // Null
                + "and someFunc() ";        // Function call
                
        //@formatter:on

        // Capture col and "" (someFunc)
        ScalarFunctionInfo someFunc = new ScalarFunctionInfo("someFunc", FunctionInfo.FunctionType.SCALAR)
        {
        };
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("col", ""))))
        {
            {
                registerFunction(someFunc);
            }
        };

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        //@formatter:off
        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        
        IPhysicalPlan expected = new DescribePlan(
                4,
                new AnalyzeInterceptor(
                    3,
                    new Filter(
                        2,
                        new AnalyzeInterceptor(
                            1,
                            new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList())
                        ),
                        new ExpressionPredicate(
                            and(
                                and(
                                    new InExpression(cre("col2", table), asList(intLit(10), intLit(20)), false),
                                    new LikeExpression(cre("col3", table), new LiteralStringExpression("string"), false, null)
                                ),
                                new NullPredicateExpression(cre("col4", table), false)))
                        )),
                true);
        //@formatter:on

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.FUNCTION_CALL, asList(new FunctionCallExpression("t", someFunc, null, asList()))),
                Triple.of(QualifiedName.of("col"), IPredicate.Type.COMPARISION, asList(intLit(10)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_predicate_pushdown_all_filters_consumed()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from \"table\" "
                + "where col = 10 "         // Comparison  
                + "and someFunc() ";        // Function call
                
        //@formatter:on

        // Capture col and "" (someFunc)
        ScalarFunctionInfo someFunc = new ScalarFunctionInfo("someFunc", FunctionInfo.FunctionType.SCALAR)
        {
        };
        TestCatalog catalog = new TestCatalog(ofEntries(entry(QualifiedName.of("table"), CollectionUtils.asSet("col", ""))))
        {
            {
                registerFunction(someFunc);
            }
        };

        catalogRegistry.registerCatalog("t", catalog);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalSelectStatement) queryStatement.getStatements()
                .get(0)).getSelect();

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "");

        //@formatter:off
        Schema expectedSchema = Schema.of(ast("", ResolvedType.of(Type.Any), table));
        
        IPhysicalPlan expected = new TableScan(0, expectedSchema, table, "test", false, catalog.scanDataSources.get(0), emptyList());
        //@formatter:on

        //@formatter:off
        assertEquals(1, catalog.consumedPredicate.size());
        assertEquals(asList(
                Triple.of(null, IPredicate.Type.FUNCTION_CALL, asList(new FunctionCallExpression("t", someFunc, null, asList()))),
                Triple.of(QualifiedName.of("col"), IPredicate.Type.COMPARISION, asList(intLit(10)))
                ), catalog.consumedPredicate.get(QualifiedName.of("table")));
        //@formatter:on

        assertEquals(expected, actual);
    }

    private QueryStatement parse(String query)
    {
        return PARSER.parseQuery(query, null);
    }

    static class TestCatalog extends Catalog
    {
        final Map<QualifiedName, Set<String>> predicateColumnsToConsume;
        final Map<QualifiedName, List<Triple<QualifiedName, IPredicate.Type, List<IExpression>>>> consumedPredicate = new HashMap<>();
        final List<IDatasource> scanDataSources = new ArrayList<>();
        final List<IDatasource> seekDataSources = new ArrayList<>();

        public TestCatalog(Map<QualifiedName, Set<String>> predicateColumnsToConsume)
        {
            super("test");
            this.predicateColumnsToConsume = predicateColumnsToConsume;
        }

        @Override
        public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
        {
            return getDataSource(session, seekPredicate.getIndex()
                    .getTable(), data, seekPredicate);
        }

        @Override
        public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
        {
            return getDataSource(session, table, data, null);
        }

        private IDatasource getDataSource(IQuerySession session, QualifiedName table, DatasourceData data, ISeekPredicate seekPredicate)
        {
            Set<String> predicateColumnsToConsume = this.predicateColumnsToConsume.getOrDefault(table, emptySet());

            Iterator<IPredicate> it = data.getPredicates()
                    .iterator();
            while (it.hasNext())
            {
                IPredicate pair = it.next();
                String col = ObjectUtils.defaultIfNull(pair.getQualifiedColumn(), QualifiedName.EMPTY)
                        .toString();
                if (predicateColumnsToConsume.contains(col))
                {
                    List<IExpression> value = null;
                    switch (pair.getType())
                    {
                        case COMPARISION:
                            value = asList(pair.getComparisonExpression());
                            break;
                        case IN:
                            value = new ArrayList<>();
                            value.add(new LiteralStringExpression(pair.getInExpression()
                                    .isNot() ? "NOT"
                                            : ""));
                            value.addAll(pair.getInExpression()
                                    .getArguments());
                            break;
                        case LIKE:
                            value = asList(new LiteralStringExpression(pair.getLikeExpression()
                                    .isNot() ? "NOT"
                                            : ""),
                                    pair.getLikeExpression()
                                            .getPatternExpression());
                            break;
                        case NULL:
                            value = asList(new LiteralStringExpression(pair.getNullPredicateExpression()
                                    .isNot() ? "NOT NULL"
                                            : "NULL"));
                            break;
                        case FUNCTION_CALL:
                            value = asList(pair.getFunctionCallExpression());
                            break;
                        case UNDEFINED:
                            value = asList(pair.getUndefinedExpression());
                            break;
                        default:
                            break;
                    }

                    consumedPredicate.computeIfAbsent(table, k -> new ArrayList<>())
                            .add(Triple.of(pair.getQualifiedColumn(), pair.getType(), value));
                    it.remove();
                }

            }

            IDatasource ds = new IDatasource()
            {
                @Override
                public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
                {
                    return null;
                }
            };

            if (seekPredicate != null)
            {
                seekDataSources.add(ds);
            }
            else
            {
                scanDataSources.add(ds);
            }
            return ds;
        }
    }
}
