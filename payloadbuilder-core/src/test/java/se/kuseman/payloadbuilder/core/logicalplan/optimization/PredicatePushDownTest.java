package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static se.kuseman.payloadbuilder.api.QualifiedName.of;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link PredicatePushDown} */
public class PredicatePushDownTest extends ALogicalPlanOptimizerTest
{
    private final ColumnResolver columnOptimizer = new ColumnResolver();
    private final PredicatePushDown predicatePushDown = new PredicatePushDown();
    private final Schema schema = Schema.of(ast("t", Type.Any, table));
    private final Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
    private final Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

    @Test
    public void test_table_and_no_filter()
    {
        ILogicalPlan plan = tableScan(schema, table);

        ILogicalPlan actual = optimize(plan);

        ILogicalPlan expected = tableScan(schema, table);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_function_and_no_filter()
    {
        TableSourceReference opencsv = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "sys", QualifiedName.of("opencsv"), "t");
        Schema schema = Schema.of(ast("t", Type.Any, opencsv));

        ILogicalPlan plan = new TableFunctionScan(opencsv, schema, asList(), asList(), null);

        ILogicalPlan actual = optimize(plan);

        ILogicalPlan expected = new TableFunctionScan(opencsv, schema, asList(), asList(), null);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_table_and_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = new Filter(
                tableScan(schema, table),
                null,
                e("t.col1")
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = new Filter(
                tableScan(schema, table),
                table,
                eq(cre("col1", table), LiteralBooleanExpression.TRUE)      // Predicate analyzer rewrites this to an equal
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_function_scan_and_surrounding_filter()
    {
        TableSourceReference opencsv = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "sys", QualifiedName.of("opencsv"), "t");
        Schema schema = Schema.of(ast("t", Type.Any, opencsv));

        //@formatter:off
        ILogicalPlan plan = new Filter(
                new TableFunctionScan(opencsv, schema, asList(), asList(), null),
                null,
                e("t.col1")
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = new Filter(
                new TableFunctionScan(opencsv, schema, asList(), asList(), null),
                opencsv,
                eq(cre("col1", opencsv), LiteralBooleanExpression.TRUE)      // Predicate analyzer rewrites this to an equal
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    /**
     * Test for a regression found were filters inside sub queries didn't get any pushed down predicates because of
     * scope issues when going down one level due to subquery.
     */
    @Test
    public void test_filter_inside_subquery()
    {
        //@formatter:off
        /*
         * select *
         * from tableA a
         * outer apply (
         *   select *
         *   from tableB b
         *   where b.col >10
         * ) x
         * where a.col1 > 10
         */
        ILogicalPlan plan = getPlanBeforeRule("""
                select *
                from tableA a
                outer apply (
                  select *
                  from tableB b
                  where b.col = 666
                ) x
                where a.col1 > 10
                """, PredicatePushDown.class);
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        TableSourceReference tableA = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", of("tableA"), "a");
        TableSourceReference tableB = new TableSourceReference(2, TableSourceReference.Type.TABLE, "", of("tableB"), "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", of("x"), "x");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected = new Projection(
                new Join(
                    new Filter(
                        tableScan(schemaA, tableA),
                        tableA,                                     // Verify filter got connected to tableA
                        gt(cre("col1", tableA), intLit(10))),
                    subQuery(
                        new Projection(
                            new Filter(
                                tableScan(schemaB, tableB),
                                tableB,                             // Verify filter got connected to tableB
                                eq(cre("col", tableB), intLit(666))),
                            asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableB))),
                            subQueryX
                        ),
                        subQueryX
                    ),
                    Join.Type.LEFT,
                    null,
                    null,
                    emptySet(),
                    false,
                    schemaA),
                asList(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA, tableB))),
                null
                );
        //@formatter:on

        // System.out.println(actual.print(0));
        // System.out.println(expected.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_join_and_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.INNER,
                                    null,
                                    e("a.col3 = b.col4"),
                                    asSet(),
                                    false,
                                    Schema.EMPTY),
                            null,
                            e("a.col7 > b.col8 AND a.col4 = 'test' AND b.col5 > 100")
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        new Filter(
                            tableScan(schemaA, tableA),
                            tableA,
                            eq(cre("col4", tableA), new LiteralStringExpression("test"))),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            gt(cre("col5", tableB), intLit(100))),
                        Join.Type.INNER,
                        null,
                        eq(cre("col3", tableA), cre("col4", tableB)),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    gt(cre("col7", tableA), cre("col8", tableB))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_join_and_surrounding_filter_2()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                tableScan(schemaA, tableA),
                                tableScan(schemaB, tableB),
                                Join.Type.INNER,
                                null,
                                e("a.col3 = b.col4"),
                                asSet(),
                                false,
                                Schema.EMPTY),
                            null,
                            e("a.col7 > b.col8 AND a.col4 = 'test'")
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        new Filter(
                            tableScan(schemaA, tableA),
                            tableA,
                            eq(cre("col4", tableA), new LiteralStringExpression("test"))),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        null,
                        eq(cre("col3", tableA), cre("col4", tableB)),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    gt(cre("col7", tableA), cre("col8", tableB))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_cross_join_with_where()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                tableScan(schemaA, tableA),
                                tableScan(schemaB, tableB),
                                Join.Type.CROSS,
                                null,
                                null,
                                asSet(),
                                false,
                                Schema.EMPTY),
                            null,
                            e("b.active")
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                    new Join(
                        tableScan(schemaA, tableA),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre("active", tableB), LiteralBooleanExpression.TRUE)),
                        Join.Type.CROSS,
                        null,
                        null,
                        asSet(),
                        false,
                        Schema.EMPTY);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_join_with_filter_and_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.INNER,
                                    null,
                                    e("a.col3 = b.col4 AND a.col10 = 'test2' AND b.col9"),
                                    asSet(),
                                    false,
                                    Schema.EMPTY),
                            null,
                            e("a.col7 > b.col8 AND a.col4 = 'test' AND b.col5 > 100")
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        new Filter(
                            tableScan(schemaA, tableA),
                            tableA,
                            and(eq(cre("col10", tableA), new LiteralStringExpression("test2")), eq(cre("col4", tableA), new LiteralStringExpression("test")))),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            and(eq(cre("col9", tableB), LiteralBooleanExpression.TRUE), gt(cre("col5", tableB), intLit(100)))),
                        Join.Type.INNER,
                        null,
                        eq(cre("col3", tableA), cre("col4", tableB)),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    gt(cre("col7", tableA), cre("col8", tableB))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_left_join_with_is_null_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.LEFT,
                                    null,
                                    e("a.col3 = b.col4 AND a.col10 = 'test2' AND b.col9"),          // a.col10 cannot be pushed down since this is left
                                    asSet(),
                                    false,
                                    Schema.EMPTY),
                            null,
                            e("b.col4 is null and a.active")     // b.col4 is null Cannot be pushed down => LEFT
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        new Filter(
                            tableScan(schemaA, tableA),
                            tableA,
                            eq(cre("active", tableA), LiteralBooleanExpression.TRUE)),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre("col9", tableB), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre("col3", tableA), cre("col4", tableB)), eq(cre("col10", tableA), new LiteralStringExpression("test2"))),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    nullP(cre("col4", tableB), false)
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_left_join_with_is_not_null_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                tableScan(schemaA, tableA),
                                tableScan(schemaB, tableB),
                                Join.Type.LEFT,
                                null,
                                e("a.col3 = b.col4 AND a.col10 = 'test2' AND b.col9"),
                                asSet(),
                                false,
                                Schema.EMPTY),
                            null,
                            e("b.col4 is not null")     // Cannot be pushed down since b is a left join
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected =
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre("col9", tableB), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre("col3", tableA), cre("col4", tableB)), eq(cre("col10", tableA), new LiteralStringExpression("test2"))),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    nullP(cre("col4", tableB), true));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_left_with_is_null_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.LEFT,
                                    null,
                                    e("a.col3 = b.col4 AND a.col10 = 'test2' AND b.col9"),          // a.col10 cannot be pushed down since left
                                    asSet(),
                                    false,
                                    Schema.EMPTY),
                            null,
                            e("b.col4 is null")     // Cannot be pushed down => OUTER + IS NULL
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre("col9", tableB), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre("col3", tableA), cre("col4", tableB)), eq(cre("col10", tableA), new LiteralStringExpression("test2"))),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    nullP(cre("col4", tableB), false)
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_left_with_is_not_null_surrounding_filter()
    {
        //@formatter:off
        ILogicalPlan plan = 
                        new Filter(
                            new Join(
                                tableScan(schemaA, tableA),
                                tableScan(schemaB, tableB),
                                Join.Type.LEFT,
                                null,
                                e("a.col3 = b.col4 AND a.col10 = 'test2' AND b.col9"),
                                asSet(),
                                false,
                                Schema.EMPTY),
                            null,
                            e("b.col4 is not null")     // Cannot be pushed down since b is a left
                        );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre("col9", tableB), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre("col3", tableA), cre("col4", tableB)), eq(cre("col10", tableA), new LiteralStringExpression("test2"))),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    null,
                    nullP(cre("col4", tableB), true));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    /**
     * Optimizes plan with predicate push down. Expressions needs to be resolved before we can analyze predicates to run it through the {@link ColumnResolver} first.
     */
    private ILogicalPlan optimize(ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = columnOptimizer.createContext(context);
        plan = columnOptimizer.optimize(ctx, plan);
        ctx = predicatePushDown.createContext(context);
        return predicatePushDown.optimize(ctx, plan);
    }
}
