package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.core.planning.QueryPlannerTest.parse;

import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.InExpression;
import se.kuseman.payloadbuilder.core.expression.LikeExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.expression.NullPredicateExpression;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeInterceptor;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeVisitor;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeVisitor.AnalyzeFormat;
import se.kuseman.payloadbuilder.core.physicalplan.AnalyzeVisitor.AnlayzeType;
import se.kuseman.payloadbuilder.core.physicalplan.DescribePlan;
import se.kuseman.payloadbuilder.core.physicalplan.ExpressionPredicate;
import se.kuseman.payloadbuilder.core.physicalplan.Filter;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.physicalplan.Projection;
import se.kuseman.payloadbuilder.core.physicalplan.TableScan;
import se.kuseman.payloadbuilder.core.planning.QueryPlannerTest.TestCatalog;
import se.kuseman.payloadbuilder.core.statement.PhysicalStatement;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;
import se.kuseman.payloadbuilder.core.utils.CollectionUtils;

/** Query planner test with analyze option. */
class QueryPlannerAnalyzeTest extends APhysicalPlanTest
{
    @BeforeEach
    void setup()
    {
        session.setDefaultCatalogAlias("t");
    }

    @Test
    void test_analyze_dont_describe_describe_plans()
    {
        //@formatter:off
        IPhysicalPlan plan = new DescribePlan(
                11,
                new AnalyzeInterceptor(
                    10,
                    new TableScan(
                    0,
                    Schema.of(ast("", table)),
                    table,
                    "test",
                    mock(IDatasource.class),
                    emptyList())),
                true,
                AnalyzeFormat.JSON,
                "");
        //@formatter:on
        assertSame(plan, AnalyzeVisitor.describe(plan, AnlayzeType.ANALYZE, AnalyzeFormat.JSON, ""));
    }

    @Test
    void test_analyze_dont_intercept_analyze_interceptors()
    {
        //@formatter:off
        IPhysicalPlan plan = new DescribePlan(
                11,
                new AnalyzeInterceptor(
                    10,
                    new TableScan(
                    0,
                    Schema.of(ast("", table)),
                    table,
                    "test",
                    mock(IDatasource.class),
                    emptyList())),
                true,
                AnalyzeFormat.JSON,
                "");
        //@formatter:on
        assertSame(plan.getChildren()
                .get(0),
                AnalyzeVisitor.describe(plan.getChildren()
                        .get(0), AnlayzeType.ANALYZE, AnalyzeFormat.JSON, "")
                        .getChildren()
                        .get(0));
    }

    @Test
    void test_describe_tableScan()
    {
        //@formatter:off
        String query = """
                describe
                select *
                from tableA
                """;
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalStatement) queryStatement.getStatements()
                .get(0)).getPlan();

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableA"), "");

        //@formatter:off
        IPhysicalPlan expected = 
                new DescribePlan(
                    1,
                    new TableScan(
                            0,
                            Schema.of(ast("", table)),
                            table,
                            "test",
                            t.scanDataSources.get(0),
                            emptyList()),
                    false, AnalyzeFormat.TABLE, "");
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFields("queryText")
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_sub_query_filter_elimination_analyze()
    {
        // Verify that the nested filters are merged when the subquery are eliminated
        //@formatter:off
        String query = """
               analyze
               select *
               from
               (
                 select *
                 from tableA a
                 where a.col1 > 10
               ) x
               where x.col > 20
               """;
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalStatement) queryStatement.getStatements()
                .get(0)).getPlan();

        TableSourceReference tableA = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableA"), "a");

        Schema expectedSchemaA = Schema.of(ast("a", tableA));

        //@formatter:off
        IPhysicalPlan expected = 
                new DescribePlan(4,
                    new AnalyzeInterceptor(
                        3,
                        new Filter(
                            1,
                            new AnalyzeInterceptor(
                                2,
                                new TableScan(0, expectedSchemaA, tableA, "test", t.scanDataSources.get(0), emptyList())
                            ),
                            new ExpressionPredicate(and(gt(cre("col1", tableA, CoreColumn.Type.NAMED_ASTERISK), intLit(10)),
                                    gt(cre("col", tableA, CoreColumn.Type.NAMED_ASTERISK), intLit(20))))
                        )),
                    true, AnalyzeFormat.TABLE, "");
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFields("queryText")
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_sub_query_projection_elimination_analyze()
    {
        // Verify that the nested projections are merged when the subquery are eliminated
        //@formatter:off
        String query = """
                analyze
                select x.col1, x.col2, x.col3 + x.col2
                from
                (
                  select col1, col2, col3
                  from tableA
                ) x
                """;
        //@formatter:on

        TestCatalog t = new TestCatalog(emptyMap());
        catalogRegistry.registerCatalog("t", t);

        QueryStatement queryStatement = parse(query);
        queryStatement = StatementPlanner.plan(session, queryStatement);

        IPhysicalPlan actual = ((PhysicalStatement) queryStatement.getStatements()
                .get(0)).getPlan();

        TableSourceReference tableA = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableA"), "");
        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema expectedSchemaA = Schema.of(ast("", tableA));

        //@formatter:off
        IPhysicalPlan expected =
                new DescribePlan(
                    5,
                    new AnalyzeInterceptor(
                        4,
                        new Projection(
                            2,
                            new AnalyzeInterceptor(
                                3,
                                new TableScan(0, expectedSchemaA, tableA, "test", t.scanDataSources.get(0), emptyList())
                            ),
                            Schema.of(
                              nast("col1", ResolvedType.ANY, tableA),
                              nast("col2", ResolvedType.ANY, tableA),
                              col("", Type.Any, "col3 + col2")
                            ),
                            List.of(cre("col1", tableA, CoreColumn.Type.NAMED_ASTERISK), cre("col2", tableA, CoreColumn.Type.NAMED_ASTERISK),
                                    add(cre("col3", tableA, CoreColumn.Type.NAMED_ASTERISK), cre("col2", tableA, CoreColumn.Type.NAMED_ASTERISK))),
                            subQueryX
                        )
                    ),
                    true, AnalyzeFormat.TABLE, "");
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFields("queryText")
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_predicate_pushdown_with_analyze()
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

        IPhysicalPlan actual = ((PhysicalStatement) queryStatement.getStatements()
                .get(0)).getPlan();

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "");

        //@formatter:off
        Schema expectedSchema = Schema.of(ast("", table));
        
        IPhysicalPlan expected = new DescribePlan(
                4,
                new AnalyzeInterceptor(
                    3,
                    new Filter(
                        1,
                        new AnalyzeInterceptor(
                            2,
                            new TableScan(0, expectedSchema, table, "test", catalog.scanDataSources.get(0), emptyList())
                        ),
                        new ExpressionPredicate(
                            and(
                                and(
                                    new InExpression(cre("col2", table, CoreColumn.Type.NAMED_ASTERISK), asList(intLit(10), intLit(20)), false),
                                    new LikeExpression(cre("col3", table, CoreColumn.Type.NAMED_ASTERISK), new LiteralStringExpression("string"), false, null)
                                ),
                                new NullPredicateExpression(cre("col4", table, CoreColumn.Type.NAMED_ASTERISK), false)))
                        )),
                true, AnalyzeFormat.TABLE, "");
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
                .ignoringFields("queryText")
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }
}
