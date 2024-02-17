package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link PredicatePushDown} */
public class PredicatePushDownTest extends ALogicalPlanOptimizerTest
{
    private final ColumnResolver columnOptimizer = new ColumnResolver();
    private final PredicatePushDown predicatePushDown = new PredicatePushDown();

    // CSOFF
    private final ColumnReference tCol = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
    private final ColumnReference aCol = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
    private final ColumnReference bCol = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
    // CSON

    private final Schema schema = Schema.of(col(tCol, Type.Any));
    private final Schema schemaA = Schema.of(col(aCol, Type.Any));
    private final Schema schemaB = Schema.of(col(bCol, Type.Any));

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
                eq(cre(tCol.rename("col1")), LiteralBooleanExpression.TRUE)      // Predicate analyzer rewrites this to an equal
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        // Assert that we actual got any paris populated
        // assertTrue(actual.getPredicatePairs().size() > 0);
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
                                    false),
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
                            eq(cre(aCol.rename("col4")), new LiteralStringExpression("test"))),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            gt(cre(bCol.rename("col5")), intLit(100))),
                        Join.Type.INNER,
                        null,
                        eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))),
                        asSet(),
                        false),
                    null,
                    gt(cre(aCol.rename("col7")), cre(bCol.rename("col8")))
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
                                false),
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
                            eq(cre(aCol.rename("col4")), new LiteralStringExpression("test"))),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        null,
                        eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))),
                        asSet(),
                        false),
                    null,
                    gt(cre(aCol.rename("col7")), cre(bCol.rename("col8")))
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
                                Join.Type.INNER,
                                null,
                                null,
                                asSet(),
                                false),
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
                            eq(cre(bCol.rename("active")), LiteralBooleanExpression.TRUE)),
                        Join.Type.INNER,
                        null,
                        null,
                        asSet(),
                        false);
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
                                    false),
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
                            and(eq(cre(aCol.rename("col10")), new LiteralStringExpression("test2")), eq(cre(aCol.rename("col4")), new LiteralStringExpression("test")))),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            and(eq(cre(bCol.rename("col9")), LiteralBooleanExpression.TRUE), gt(cre(bCol.rename("col5")), intLit(100)))),
                        Join.Type.INNER,
                        null,
                        eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))),
                        asSet(),
                        false),
                    null,
                    gt(cre(aCol.rename("col7")), cre(bCol.rename("col8")))
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
                                    false),
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
                            eq(cre(aCol.rename("active")), LiteralBooleanExpression.TRUE)),
                        new Filter(
                            tableScan(schemaB, tableB),
                            tableB,
                            eq(cre(bCol.rename("col9")), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))), eq(cre(aCol.rename("col10")), new LiteralStringExpression("test2"))),
                        asSet(),
                        false),
                    null,
                    nullP(cre(bCol.rename("col4")), false)
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
                                false),
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
                            eq(cre(bCol.rename("col9")), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))), eq(cre(aCol.rename("col10")), new LiteralStringExpression("test2"))),
                        asSet(),
                        false),
                    null,
                    nullP(cre(bCol.rename("col4")), true));
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
                                    false),
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
                            eq(cre(bCol.rename("col9")), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))), eq(cre(aCol.rename("col10")), new LiteralStringExpression("test2"))),
                        asSet(),
                        false),
                    null,
                    nullP(cre(bCol.rename("col4")), false)
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
                                false),
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
                            eq(cre(bCol.rename("col9")), LiteralBooleanExpression.TRUE)),
                        Join.Type.LEFT,
                        null,
                        and(eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))), eq(cre(aCol.rename("col10")), new LiteralStringExpression("test2"))),
                        asSet(),
                        false),
                    null,
                    nullP(cre(bCol.rename("col4")), true));
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
